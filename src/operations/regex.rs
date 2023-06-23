use std::borrow::Cow;
use std::cell::RefCell;

use bstring::bstr;
use lazy_static::{__Deref, lazy_static};
use nonmax::NonMaxUsize;
use regex;
use regex::bytes;

use crate::field_data::fd_command_buffer::{FDCommandBuffer, FieldActionKind};
use crate::field_data::fd_iter::{
    iterate_typed_slice_for_inline_bytes, iterate_typed_slice_for_inline_text,
};
use crate::field_data::fd_push_interface::FDPushInterface;
use crate::field_data::RunLength;
use crate::utils::universe::Universe;
use crate::worker_thread_session::Field;
use crate::{
    field_data::fd_iter::FDTypedSlice,
    field_data::{fd_iter::FDIterator, fd_iter_hall::FDIterId, field_value_flags, FieldReference},
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData, MatchSetId},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    transform::{TransformData, TransformId},
};

pub struct OpRegex {
    pub regex: bytes::Regex,
    pub text_only_regex: Option<regex::Regex>,
    pub multimatch: bool,
    pub capture_group_names: Vec<StringStoreEntry>,
}

pub struct TfRegex {
    pub regex: bytes::Regex,
    pub capture_locs: bytes::CaptureLocations,
    pub text_only_regex: Option<(regex::Regex, regex::CaptureLocations)>,
    pub capture_group_fields: Vec<FieldId>,
    pub input_field_iter_id: FDIterId,
    pub multimatch: bool,
}

struct RegexBatchState {
    drop_count: usize,
    field_idx: usize,
    match_count: usize,
}

#[derive(Default)]
pub struct RegexOptions {
    // makes ^ and $ match lines in addition to start / end of stream
    // (commonly called multiline)
    pub line_based: bool,

    // return multiple matches instead of only the first
    pub multimatch: bool,

    // makes . match \n
    pub dotall: bool,

    // enables case insensitive matches
    pub case_insensitive: bool,

    // disable unicode for character classes making e.g. \w  only
    // match ascii characters
    // for byte sequences, having this disabled means that '.' will match
    // any byte sequence that looks like a valid UTF-8 character
    pub ascii_mode: bool,
}

lazy_static! {
    static ref REGEX_CAPTURE_GROUP_REGEX: bytes::Regex = bytes::Regex::new(
        r"(?:^|[^\\])(?:[^\\]|\\\\)*\(\?P?<(?<capture_group_name>([^>\\]|\\[^])*)>"
    )
    .unwrap();
}

pub fn parse_regex_op(
    string_store: &mut StringStore,
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
    opts: RegexOptions,
) -> Result<OpRegex, OperatorCreationError> {
    let regex;
    let text_only_regex;

    if let Some(value) = value {
        match value.to_str() {
            Err(_) => {
                return Err(OperatorCreationError::new(
                    "regex pattern must be legal UTF-8",
                    arg_idx,
                ));
            }
            Ok(value) => {
                regex = bytes::RegexBuilder::new(value)
                    .multi_line(opts.line_based)
                    .dot_matches_new_line(opts.dotall)
                    .case_insensitive(opts.case_insensitive)
                    .unicode(!opts.ascii_mode)
                    .build()
                    .map_err(|e| OperatorCreationError {
                        message: Cow::Owned(format!("failed to compile regex: {}", e)),
                        cli_arg_idx: arg_idx,
                    })?;
                text_only_regex = Some(
                    regex::RegexBuilder::new(value)
                        .multi_line(opts.line_based)
                        .dot_matches_new_line(opts.dotall)
                        .case_insensitive(opts.case_insensitive)
                        .unicode(!opts.ascii_mode)
                        .build()
                        .map_err(|e| OperatorCreationError {
                            message: Cow::Owned(format!("failed to compile regex: {}", e)),
                            cli_arg_idx: arg_idx,
                        })?,
                );
            }
        }
    } else {
        return Err(OperatorCreationError::new(
            "the regex operator needs a regular expression as an argument",
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
        text_only_regex,
        capture_group_names,
        multimatch: opts.multimatch,
    })
}

pub fn setup_tf_regex<'a>(
    sess: &mut JobData,
    ms_id: MatchSetId,
    input_field: FieldId,
    op: &'a OpRegex,
) -> (TransformData<'a>, FieldId) {
    let mut cgfs: Vec<FieldId> = op
        .capture_group_names
        .iter()
        .map(|name| sess.entry_data.add_field(ms_id, Some(*name)))
        .collect();
    cgfs.sort_unstable();
    let output_field = cgfs[0];
    let re = TfRegex {
        regex: op.regex.clone(),
        text_only_regex: op
            .text_only_regex
            .as_ref()
            .map(|r| (r.clone(), r.capture_locations())),
        capture_group_fields: cgfs,
        capture_locs: op.regex.capture_locations(),
        multimatch: op.multimatch,
        input_field_iter_id: sess.entry_data.fields[input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
    };
    (TransformData::Regex(re), output_field)
}

enum AnyRegex<'a, 'b> {
    Text(
        &'a mut regex::Regex,
        &'a mut regex::CaptureLocations,
        &'b str,
    ),
    Bytes(
        &'a mut bytes::Regex,
        &'a mut bytes::CaptureLocations,
        &'b [u8],
    ),
}

impl<'a, 'b> AnyRegex<'a, 'b> {
    fn captures_read_at(&mut self, start: usize) -> bool {
        match self {
            AnyRegex::Text(re, cl, input) => re.captures_read_at(cl, input, start).is_some(),
            AnyRegex::Bytes(re, cl, input) => re.captures_read_at(cl, input, start).is_some(),
        }
    }
    fn captures_locs_len(&mut self) -> usize {
        match self {
            AnyRegex::Text(_re, cl, _input) => cl.len(),
            AnyRegex::Bytes(_re, cl, _input) => cl.len(),
        }
    }
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)> {
        match self {
            AnyRegex::Text(_re, cl, _input) => cl.get(i),
            AnyRegex::Bytes(_re, cl, _input) => cl.get(i),
        }
    }
}

fn match_regex_inner<'a, 'b, 'c>(
    input_field_id: FieldId,
    run_length: RunLength,
    mut regex: AnyRegex<'a, 'b>,
    capture_group_fields: &Vec<FieldId>,
    multimatch: bool,
    rbs: &mut RegexBatchState,
    fields: &Universe<NonMaxUsize, RefCell<Field>>,
    command_buffer: &mut FDCommandBuffer,
) {
    let mut end_of_last_match = 0;
    let mut match_count: RunLength = 0;
    while regex.captures_read_at(end_of_last_match) {
        match_count += 1;
        rbs.field_idx -= rbs.drop_count;
        command_buffer.push_action_with_usize_rl(
            FieldActionKind::Drop,
            rbs.field_idx,
            rbs.drop_count,
        );
        rbs.drop_count = 0;
        for c in 0..regex.captures_locs_len() {
            let field = &mut fields[capture_group_fields[c]].borrow_mut().field_data;
            if let Some((cg_begin, cg_end)) = regex.captures_locs_get(c) {
                field.push_reference(
                    FieldReference {
                        field: input_field_id,
                        begin: cg_begin,
                        end: cg_end,
                    },
                    run_length as usize,
                    true,
                    true,
                );
            } else {
                field.push_null(run_length as usize, true);
            }
        }
        rbs.field_idx += 1;
        if !multimatch {
            break;
        }
        let end = regex.captures_locs_get(0).unwrap().1;
        if end == end_of_last_match {
            end_of_last_match += 1;
        } else {
            end_of_last_match = end;
        }
    }
    if match_count == 0 {
        rbs.field_idx -= rbs.drop_count + 1;
        command_buffer.push_action_with_usize_rl(
            FieldActionKind::Drop,
            rbs.field_idx,
            rbs.drop_count + 1,
        );
        rbs.drop_count = 0;
    } else if match_count > 1 {
        debug_assert!(rbs.drop_count == 0);
        command_buffer.push_action(FieldActionKind::Dup, rbs.field_idx, match_count - 1);
    }
    rbs.match_count += match_count as usize;
}

pub fn handle_tf_regex_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field_id) = sess.tf_mgr.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id;
    let command_buffer = &mut sess.entry_data.match_sets[tf.match_set_id].command_buffer;
    command_buffer.begin_action_set(tf.ordering_id.into());
    let input_field = sess.entry_data.fields[input_field_id].borrow_mut();
    let mut iter = input_field
        .deref()
        .field_data
        .get_iter(re.input_field_iter_id)
        .bounded(0, batch);

    let mut rbs = RegexBatchState {
        drop_count: 0,
        field_idx: iter.get_next_field_pos(),
        match_count: 0,
    };

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                iterate_typed_slice_for_inline_text(text, range.headers, |v, rl| {
                    let any_regex = if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                        AnyRegex::Text(regex, capture_locs, v)
                    } else {
                        AnyRegex::Bytes(&mut re.regex, &mut re.capture_locs, v.as_bytes())
                    };
                    match_regex_inner(
                        tf_id,
                        rl,
                        any_regex,
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.entry_data.fields,
                        command_buffer,
                    );
                });
            }
            FDTypedSlice::BytesInline(bytes) => {
                iterate_typed_slice_for_inline_bytes(bytes, range.headers, |v, rl| {
                    match_regex_inner(
                        input_field_id,
                        rl,
                        AnyRegex::Bytes(&mut re.regex, &mut re.capture_locs, v),
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.entry_data.fields,
                        command_buffer,
                    );
                });
            }
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
            | FDTypedSlice::Reference(_)
            | FDTypedSlice::Error(_)
            | FDTypedSlice::Html(_)
            | FDTypedSlice::StreamValueId(_)
            | FDTypedSlice::Object(_) => {
                rbs.field_idx += range.field_count;
                for cgi in &re.capture_group_fields {
                    sess.entry_data.fields[*cgi]
                        .borrow_mut()
                        .field_data
                        .push_error(
                            OperatorApplicationError::new("regex type error", op_id),
                            range.field_count,
                            true,
                            true,
                        );
                }
            }
        }
    }
    input_field
        .field_data
        .store_iter(re.input_field_iter_id, iter);
    drop(input_field);
    sess.entry_data.batch_consumed(input_field_id, batch);
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
}
