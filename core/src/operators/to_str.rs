use std::sync::Arc;

use bstr::ByteSlice;

use crate::{
    cli::{call_expr::CallExpr, CliArgumentError},
    job::JobData,
    options::{
        chain_settings::SettingStreamBufferSize,
        session_setup::SessionSetupData,
    },
    record_data::{
        field_value::FieldValue,
        field_value_ref::FieldValueSlice,
        iter::{
            field_iterator::FieldIterator,
            ref_iter::{AutoDerefIter, RefAwareFieldValueRangeIter},
        },
        iter_hall::FieldIterId,
        push_interface::PushInterface,
        stream_value::{StreamValueData, StreamValueId, StreamValueUpdate},
    },
    utils::{
        encoding::{
            self, utf8_surrocate_escape, UTF8_REPLACEMENT_CHARACTER_BYTES,
        },
        indexing_type::IndexingType,
        retain_vec_range,
    },
};

use super::{
    errors::OperatorApplicationError,
    operator::{Operator, TransformInstatiation},
    transform::{Transform, TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpToStr {
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    convert_errors: bool,
}

pub fn parse_op_to_str(
    sess: &SessionSetupData,
    mut expr: CallExpr,
) -> Result<Box<dyn Operator>, CliArgumentError> {
    // this should not happen in the cli parser because it checks using
    // `argument_matches_data_inserter`
    let mut force = false;

    expr.split_flags_arg_normalized(&sess.string_store, true);
    let (flags, args) = expr.split_flags_arg(true);

    if let Some(flags) = flags {
        for (key, value) in flags {
            let FieldValue::Argument(arg) = value else {
                unreachable!()
            };
            if key == "f" || key == "force" {
                force = true;
            } else {
                return Err(expr.error_flag_unsupported(key, arg.span));
            }
            if arg.value != FieldValue::Undefined {
                return Err(expr.error_reject_flag_value(key, arg.span));
            }
        }
    }

    if !args.is_empty() {
        return Err(expr.error_positional_args_unsupported(args[0].span));
    }

    Ok(create_op_to_str(None, force))
}

pub fn create_op_to_str(
    invalid_unicode_handler: Option<InvalidUnicodeHandler>,
    convert_errors: bool,
) -> Box<dyn Operator> {
    Box::new(OpToStr {
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
    batch_iter: FieldIterId,
    pending_streams: usize,
    invalid_unicode_handler: Box<dyn InvalidUnicodeHandlerFn>,
    convert_errors: bool,
    stream_buffer_size: usize,
}

impl Operator for OpToStr {
    fn default_name(&self) -> super::operator::OperatorName {
        "to_str".into()
    }

    fn output_count(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &crate::context::SessionData,
        _op_id: super::operator::OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &crate::context::SessionData,
        _ld: &mut crate::liveness_analysis::LivenessData,
        _op_offset_after_last_write: super::operator::OffsetInChain,
        _op_id: super::operator::OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut crate::liveness_analysis::OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: super::operator::OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let replacement_fn: Box<dyn InvalidUnicodeHandlerFn> = match &self
            .invalid_unicode_handler
        {
            Some(h) => match h {
                InvalidUnicodeHandler::Lossy => {
                    |_pos,
                     _input: &[u8],
                     out: &mut Vec<u8>|
                     -> Result<(), String> {
                        out.extend_from_slice(
                            &UTF8_REPLACEMENT_CHARACTER_BYTES,
                        );
                        Ok(())
                    }
                    .clone_dyn()
                }
                InvalidUnicodeHandler::SurrogateEscape => {
                    |_pos,
                     input: &[u8],
                     out: &mut Vec<u8>|
                     -> Result<(), String> {
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

        let stream_buffer_size =
            jd.get_setting_from_tf_state::<SettingStreamBufferSize>(tf_state);

        TransformInstatiation::Single(TransformData::from_custom(TfToStr {
            batch_iter: jd.claim_iter_for_tf_state(tf_state),
            pending_streams: 0,
            invalid_unicode_handler: replacement_fn,
            convert_errors: self.convert_errors,
            stream_buffer_size,
        }))
    }
}

impl Transform<'_> for TfToStr {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let input_field_id = tf.input_field;
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, tf.input_field);

        let mut output_field =
            jd.field_mgr.fields[tf.output_field].borrow_mut();

        let ofd = &mut output_field.iter_hall;
        let base_iter = jd
            .field_mgr
            .lookup_iter(tf.input_field, &input_field, self.batch_iter)
            .bounded(0, batch_size);
        let starting_pos = base_iter.get_next_field_pos();

        let mut iter =
            AutoDerefIter::new(&jd.field_mgr, tf.input_field, base_iter);

        while let Some(range) = iter.next_range(&jd.match_set_mgr) {
            match range.base.data {
                FieldValueSlice::Error(errs) => {
                    if self.convert_errors {
                    } else {
                        for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                            &range, errs,
                        ) {
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
            .store_iter(input_field_id, self.batch_iter, iter_base);
        drop(input_field);
        drop(output_field);
        let streams_done = self.pending_streams == 0;

        if streams_done && ps.next_batch_ready {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
        jd.tf_mgr.submit_batch(
            tf_id,
            consumed_fields,
            ps.group_to_truncate,
            ps.input_done,
        );
    }

    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData<'_>,
        update: StreamValueUpdate,
    ) {
        let op_id = jd.tf_mgr.transforms[update.tf_id].op_id.unwrap();

        let sv_in_id = update.sv_id;
        let sv_out_id = StreamValueId::from_usize(update.custom);
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
            sv_out.data_inserter(sv_out_id, self.stream_buffer_size, true);

        while let Some(mut data) =
            iter.next_steal(inserter.may_append_buffer())
        {
            let input_data = match &mut data {
                StreamValueData::Text { .. }
                | StreamValueData::StaticText(_) => {
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
                        if let Ok(text) =
                            String::from_utf8(std::mem::take(data))
                        {
                            inserter
                                .append(StreamValueData::from_string(text));
                            continue;
                        }
                    }
                    &data[range.clone()]
                }
                StreamValueData::Single(_) => todo!(),
            };
            let res = inserter
                .with_text_buffer(|buf| {
                    encoding::decode_to_utf8(
                        &mut encoding_rs::UTF_8
                            .new_decoder_without_bom_handling(),
                        input_data,
                        &mut self.invalid_unicode_handler,
                        unsafe { buf.as_mut_vec() },
                        input_done,
                    )
                })
                .unwrap();
            if let Err(message) = res {
                drop(inserter);
                sv_out.done = true;
                sv_out.error = Some(Arc::new(
                    OperatorApplicationError::new_s(message, op_id),
                ));
                jd.sv_mgr.inform_stream_value_subscribers(sv_out_id);
                jd.sv_mgr.drop_field_value_subscription(
                    sv_in_id,
                    Some(update.tf_id),
                );
                return;
            }
        }
    }
}
