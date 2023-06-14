use bstring::bstr;
use regex::{CaptureLocations, Regex};

use crate::{
    field_data::{field_value_flags, FieldData, FieldReference},
    field_data_iterator::{FDIterator, FDTypedSlice},
    options::argument::CliArgIdx,
    string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{Field, FieldId, JobData, MatchSetId, WorkerThreadSession},
};

use super::{
    transform_state::{TransformData, TransformId},
    OperatorApplicationError, OperatorCreationError,
};

pub struct OpRegex {
    pub regex: Regex,
    pub capture_group_names: Vec<StringStoreEntry>,
}

pub struct TfRegex {
    pub regex: Regex,
    pub capture_locs: CaptureLocations,
    pub capture_group_fields: Vec<FieldId>,
}

pub fn parse_regex_op(
    string_store: &mut StringStore,
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OpRegex, OperatorCreationError> {
    let regex = if let Some(value) = value {
        match value.to_str() {
            //TODO: we should support byte regex
            Err(_) => {
                return Err(OperatorCreationError::new(
                    "regex pattern must be legal UTF-8",
                    arg_idx,
                ));
            }
            Ok(value) => Regex::new(value)
                .map_err(|_| OperatorCreationError::new("failed to compile regex", arg_idx))?,
        }
    } else {
        return Err(OperatorCreationError::new(
            "regex needs an argument",
            arg_idx,
        ));
    };
    let mut unnamed_capture_groups: usize = 0;
    let capture_group_names = regex
        .capture_names()
        .into_iter()
        .map(|name| match name {
            Some(name) => string_store.intern_cloned(name),
            None => {
                let id = string_store.intern_moved(unnamed_capture_groups.to_string());
                unnamed_capture_groups += 1;
                id
            }
        })
        .collect();

    Ok(OpRegex {
        regex,
        capture_group_names,
    })
}

pub fn setup_tf_regex<'a>(
    sess: &mut WorkerThreadSession,
    ms_id: MatchSetId,
    _input_field: FieldId,
    op: &'a OpRegex,
) -> (TransformData<'a>, FieldId) {
    let mut cgfs: Vec<FieldId> = op
        .capture_group_names
        .iter()
        .map(|name| sess.add_field(ms_id, Some(*name)))
        .collect();
    cgfs.sort();
    let output_field = cgfs[0];
    let re = TfRegex {
        regex: op.regex.clone(),
        capture_group_fields: cgfs,
        capture_locs: op.regex.capture_locations(),
    };
    (TransformData::Regex(re), output_field)
}

pub fn handle_tf_regex_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field) = sess.claim_batch(tf_id);
    let op_id = sess.transforms[tf_id].op_id;
    let input_field_ref = &sess.fields[input_field];
    let field_ref = unsafe {
        // HACK // EVIL: this is UB
        std::mem::transmute::<*mut Field, &mut Field>(std::mem::transmute::<&Field, *mut Field>(
            input_field_ref,
        ))
    };
    let mut iter = field_ref.field_data.iter();
    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                let mut data_start = 0usize;
                let mut data_end = 0usize;
                for h in range.headers.iter() {
                    data_end += h.size as usize;
                    if re
                        .regex
                        .captures_read(&mut re.capture_locs, &text[data_start..data_end])
                        .is_some()
                    {
                        for c in 0..re.capture_locs.len() {
                            if let Some((cg_begin, cg_end)) = re.capture_locs.get(c) {
                                sess.fields[re.capture_group_fields[c]]
                                    .field_data
                                    .push_reference(
                                        FieldReference {
                                            field: input_field,
                                            begin: cg_begin,
                                            end: cg_end,
                                        },
                                        h.run_length as usize,
                                    );
                            }
                        }
                    } else {
                        for f in re.capture_group_fields.iter() {
                            sess.fields[*f].field_data.push_null(h.run_length as usize);
                        }
                    }

                    data_start = data_end;
                }
            }
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
            | FDTypedSlice::Reference(_)
            | FDTypedSlice::Error(_)
            | FDTypedSlice::Html(_)
            | FDTypedSlice::BytesInline(_)
            | FDTypedSlice::StreamValueId(_)
            | FDTypedSlice::Object(_) => {
                for f in re.capture_group_fields.iter() {
                    let field_ref = &sess.fields[*f].field_data;
                    unsafe {
                        // HACK // EVIL: this is UB
                        std::mem::transmute::<*mut FieldData, &mut FieldData>(
                            std::mem::transmute::<&FieldData, *mut FieldData>(field_ref),
                        )
                    }
                    .push_error(
                        OperatorApplicationError::new("regex type error", op_id),
                        range.field_count,
                    );
                }
            }
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
