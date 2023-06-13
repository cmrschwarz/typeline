use bstring::bstr;
use regex::{CaptureLocations, Regex};

use crate::{
    field_data::{field_value_flags, FieldData},
    field_data_iterator::{FDIterator, FDTypedRange, FDTypedSlice},
    options::argument::CliArgIdx,
    string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData, MatchSetId, TransformId, WorkerThreadSession},
};

use super::{OperatorApplicationError, OperatorCreationError};

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
                unnamed_capture_groups += 1;
                string_store.intern_moved(unnamed_capture_groups.to_string())
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
    op: &'a OpRegex,
) -> TfRegex {
    TfRegex {
        regex: op.regex.clone(),
        capture_group_fields: op
            .capture_group_names
            .iter()
            .map(|name| sess.add_field(ms_id, Some(*name)))
            .collect(),
        capture_locs: op.regex.capture_locations(),
    }
}

/*
fn process(
    &mut self,
    _ctx: &SessionData,
    _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
    tfo: &TransformOutput,
    output: &mut VecDeque<TransformOutput>,
) -> Result<(), TransformApplicationError> {
    if tfo.is_last_chunk.is_some() {
        return Err(TransformApplicationError::new(
            "the regex transform does not support streams",
            self.op_ref,
        ));
    }
    match &tfo.data {
        Some(MatchData::Text(text)) => {
            let mut match_index = tfo.match_index;
            for cap in self.regex.captures_iter(text) {
                output.push_back(TransformOutput {
                    match_index,
                    data: Some(MatchData::Text(cap.get(0).unwrap().as_str().to_owned())),
                    args: Vec::default(), //todo
                    is_last_chunk: None,
                });
                match_index += 1;
            }
            Ok(())
        }
        Some(md) => Err(TransformApplicationError {
            message: format!(
                "the regex transform does not support match data kind '{}'",
                md.kind().to_str()
            ),
            op_ref: self.op_ref,
        }),
        None => Err(TransformApplicationError::new(
            "unexpected none match for regex transform",
            self.op_ref,
        )),
    }
}
*/

pub fn handle_tf_regex_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field) = sess.claim_batch(tf_id);
    let op_id = sess.transforms[tf_id].op_id;
    let mut iter = sess.fields[input_field].field_data.iter();
    iter.prev_n_fields(batch);
    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
            | FDTypedSlice::Reference(_)
            | FDTypedSlice::Error(_)
            | FDTypedSlice::Html(_)
            | FDTypedSlice::BytesInline(_)
            | FDTypedSlice::TextInline(_)
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
