use std::borrow::Cow;

use bstring::bstr;
use lazy_static::lazy_static;
use regex;
use regex::bytes;

use crate::{
    field_data::fd_iter::FDTypedSlice,
    field_data::{
        fd_iter::{FDIterMut, FDIterator},
        field_value_flags, FieldReference,
    },
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
    pub multimatch: bool,
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
    _input_field: FieldId,
    op: &'a OpRegex,
) -> (TransformData<'a>, FieldId) {
    let mut cgfs: Vec<FieldId> = op
        .capture_group_names
        .iter()
        .map(|name| sess.entry_data.add_field(ms_id, Some(*name)))
        .collect();
    cgfs.sort();
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
    };
    (TransformData::Regex(re), output_field)
}

macro_rules! match_regex_inner {
    (   $sess: expr,             // &JobData
        $tf_id: expr,            // TransformId
        $re: expr,               // &mut TfRegex
        $input: expr,            // &str / &u8
        $input_field_id: expr,   // usize
        $run_length: expr,       // usize
        $regex: expr,            // &mut Regex / &mut bytes::Regex
        $capture_locs: expr,     // &mut CaptureLocations / &mut bytes::CaptureLocations
        $field_index: expr,      // usize
        $drop_count: expr,       // usize
        $fd_iter_with_ref: expr  // FDIterWithRef
    ) => {{
        let mut end_of_last_match = 0;
        let mut fd_iter = $fd_iter_with_ref;
        let mut fidx = $field_index;
        let mut dc = $drop_count;
        let mut match_count = 0;
        while $regex
            .captures_read_at($capture_locs, $input, end_of_last_match)
            .is_some()
        {
            match_count += 1;
            if $drop_count > 0 {
                fd_iter = $sess.drop_n_entries_at($tf_id, $input_field_id, dc, fd_iter);
                fidx -= dc;
                dc = 0;
            }
            for c in 0..$capture_locs.len() {
                let field = &mut $sess.entry_data.fields[$re.capture_group_fields[c]]
                    .borrow_mut()
                    .field_data;
                if let Some((cg_begin, cg_end)) = $capture_locs.get(c) {
                    field.push_reference(
                        FieldReference {
                            field: $input_field_id,
                            begin: cg_begin,
                            end: cg_end,
                        },
                        $run_length as usize,
                    );
                } else {
                    field.push_null($run_length as usize);
                }
            }
            if !$re.multimatch {
                break;
            }
            let end = $capture_locs.get(0).unwrap().1;
            if end == end_of_last_match {
                end_of_last_match += 1;
            } else {
                end_of_last_match = end;
            }
        }
        fidx += match_count;
        if match_count == 0 {
            fd_iter = $sess.drop_n_entries_at($tf_id, $input_field_id, dc + 1, fd_iter);
            fidx -= dc;
            dc = 0;
        }
        (fidx, dc, fd_iter)
    }};
}

pub fn handle_tf_regex_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field_id) = sess.tf_mgr.claim_batch(tf_id);
    let op_id = sess.tf_mgr.transforms[tf_id].op_id;

    let mut input_field = sess.entry_data.fields[input_field_id].borrow_mut();
    let mut fd_iter = FDIterMut::from_start(&mut input_field.field_data);

    let mut field_index = 0;
    let mut drop_count = 0;
    while let Some(range) = fd_iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        field_index += range.field_count;
        match range.data {
            FDTypedSlice::TextInline(text) => {
                let mut data_start = 0usize;
                let mut data_end = 0usize;
                for (i, h) in range.headers.iter().enumerate() {
                    data_end += h.size as usize;
                    if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                        (field_index, drop_count, fd_iter) = match_regex_inner!(
                            sess,
                            tf_id,
                            re,
                            &text[data_start..data_end],
                            input_field_id,
                            range.run_length(i),
                            regex,
                            capture_locs,
                            field_index,
                            drop_count,
                            fd_iter
                        );
                    } else {
                        (field_index, drop_count, fd_iter) = match_regex_inner!(
                            sess,
                            tf_id,
                            re,
                            &text[data_start..data_end].as_bytes(),
                            input_field_id,
                            range.run_length(i),
                            &mut re.regex,
                            &mut re.capture_locs,
                            field_index,
                            drop_count,
                            fd_iter
                        );
                    }
                    data_start = data_end;
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                let mut data_start = 0usize;
                let mut data_end = 0usize;

                for (i, h) in range.headers.iter().enumerate() {
                    data_end += h.size as usize;
                    (field_index, drop_count, fd_iter) = match_regex_inner!(
                        sess,
                        tf_id,
                        re,
                        &bytes[data_start..data_end],
                        input_field_id,
                        range.run_length(i),
                        &mut re.regex,
                        &mut re.capture_locs,
                        field_index,
                        drop_count,
                        fd_iter
                    );
                    data_start = data_end;
                }
            }
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
            | FDTypedSlice::Reference(_)
            | FDTypedSlice::Error(_)
            | FDTypedSlice::Html(_)
            | FDTypedSlice::StreamValueId(_)
            | FDTypedSlice::Object(_) => {
                for f in re.capture_group_fields.iter() {
                    sess.entry_data.fields[*f]
                        .borrow_mut()
                        .field_data
                        .push_error(
                            OperatorApplicationError::new("regex type error", op_id),
                            range.field_count,
                        );
                }
            }
        }
    }
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
}
