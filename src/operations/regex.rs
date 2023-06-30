use arrayvec::ArrayString;
use bstr::{BStr, ByteSlice};
use nonmax::NonMaxUsize;
use regex::{self, bytes};
use smallstr::SmallString;
use std::borrow::Cow;
use std::cell::RefCell;
use std::ops::Deref;

use std::num::NonZeroUsize;

use crate::fd_ref_iter::FDRefIterLazy;
use crate::field_data::fd_command_buffer::{FDCommandBuffer, FieldActionKind};
use crate::field_data::fd_iter::{FDTypedValue, InlineBytesIter, InlineTextIter};
use crate::field_data::fd_push_interface::FDPushInterface;
use crate::field_data::RunLength;
use crate::utils::universe::Universe;
use crate::utils::{self, USIZE_MAX_DECIMAL_DIGITS};
use crate::worker_thread_session::Field;
use crate::{
    field_data::fd_iter::FDTypedSlice,
    field_data::{fd_iter::FDIterator, fd_iter_hall::FDIterId, field_value_flags, FieldReference},
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData, MatchSetId},
};

use super::errors::OperatorSetupError;
use super::operator::OperatorData;
use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    transform::{TransformData, TransformId},
};

pub struct OpRegex {
    pub regex: bytes::Regex,
    pub text_only_regex: Option<regex::Regex>,
    pub opts: RegexOptions,
    pub output_group_id: usize,
    pub capture_group_names: Vec<Option<StringStoreEntry>>,
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
    // disable unicode for character classes making e.g. \w  only
    // match ascii characters
    // for byte sequences, having this disabled means that '.' will match
    // any byte sequence that looks like a valid UTF-8 character
    pub ascii_mode: bool,

    // return multiple matches instead of only the first
    pub multimatch: bool,

    // makes ^ and $ match lines in addition to start / end of stream
    // (commonly called multiline)
    pub line_based: bool,

    // makes . match \n
    pub dotall: bool,

    // enables case insensitive matches
    pub case_insensitive: bool,
}

impl RegexOptions {
    pub fn default_op_name(&self) -> SmallString<[u8; 16]> {
        let mut res = SmallString::from_str("r");
        if self.ascii_mode {
            res.push('b');
        }
        if self.multimatch {
            res.push('m');
        }
        if self.line_based {
            res.push('l');
        }
        if self.dotall {
            res.push('d');
        }
        if self.case_insensitive {
            res.push('i');
        }
        res
    }
}

const MAX_DEFAULT_CAPTURE_GROUP_NAME_LEN: usize = USIZE_MAX_DECIMAL_DIGITS + 1;

pub fn preparse_replace_empty_capture_group<'a>(
    regex_str: &'a str,
    opts: &RegexOptions,
) -> Result<
    (
        Cow<'a, str>,
        Option<ArrayString<MAX_DEFAULT_CAPTURE_GROUP_NAME_LEN>>,
    ),
    Cow<'static, str>,
> {
    let parser = regex_syntax::ParserBuilder::new()
        .multi_line(opts.line_based)
        .dot_matches_new_line(opts.dotall)
        .case_insensitive(opts.case_insensitive)
        .unicode(!opts.ascii_mode)
        .build();
    let mut re = Cow::Borrowed(regex_str);
    let mut parse_res = parser.clone().parse(&re);
    let mut opt_empty_group_replacement_str = None;
    if let Err(regex_syntax::Error::Parse(pe)) = parse_res {
        if let regex_syntax::ast::ErrorKind::GroupNameEmpty = pe.kind() {
            let mut owned = re.into_owned();
            let empty_group_span = *pe.span();
            let mut span = empty_group_span;
            let mut empty_group_replacement_str = ArrayString::new();
            loop {
                empty_group_replacement_str.clear();
                empty_group_replacement_str.push('_');
                empty_group_replacement_str
                    .push_str(&utils::non_zero_usize_to_str(rand::random::<NonZeroUsize>()));
                owned.replace_range(
                    span.start.offset..span.end.offset,
                    &empty_group_replacement_str,
                );
                parse_res = parser.clone().parse(&owned);
                if let Err(regex_syntax::Error::Parse(pe)) = &parse_res {
                    match pe.kind() {
                        regex_syntax::ast::ErrorKind::GroupNameEmpty => {
                            return Err(Cow::Borrowed(
                                "the regex can only contain one group with an empty name",
                            ));
                        }
                        regex_syntax::ast::ErrorKind::GroupNameDuplicate { original: og_span } => {
                            if og_span.start.offset == empty_group_span.start.offset {
                                span = *pe.span();
                                continue;
                            }
                            break;
                        }
                        _ => break,
                    }
                }
                break;
            }
            opt_empty_group_replacement_str = Some(empty_group_replacement_str);
            re = Cow::Owned(owned);
        }
    }
    return Ok((re, opt_empty_group_replacement_str));
}

pub fn parse_op_regex(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
    opts: RegexOptions,
) -> Result<OpRegex, OperatorCreationError> {
    let regex;
    let text_only_regex;
    let mut output_group_id = 0;

    if let Some(value) = value {
        match value.to_str() {
            Err(_) => {
                return Err(OperatorCreationError::new(
                    "regex pattern must be legal UTF-8",
                    arg_idx,
                ));
            }
            Ok(value) => {
                let (re, empty_group_replacement) =
                    preparse_replace_empty_capture_group(value, &opts).map_err(|e| {
                        OperatorCreationError {
                            cli_arg_idx: arg_idx,
                            message: e,
                        }
                        .into()
                    })?;

                regex = bytes::RegexBuilder::new(&re)
                    .multi_line(opts.line_based)
                    .dot_matches_new_line(opts.dotall)
                    .case_insensitive(opts.case_insensitive)
                    .unicode(!opts.ascii_mode)
                    .build()
                    .map_err(|e| OperatorCreationError {
                        message: Cow::Owned(format!("failed to compile regex: {}", e)),
                        cli_arg_idx: arg_idx,
                    })?;

                if let Some(egr) = empty_group_replacement {
                    output_group_id = regex
                        .capture_names()
                        .enumerate()
                        .find(|(_i, cn)| cn.is_some_and(|cn| cn == &egr))
                        .map(|(i, _cn)| i)
                        .unwrap();
                }

                text_only_regex = regex::RegexBuilder::new(&re)
                    .multi_line(opts.line_based)
                    .dot_matches_new_line(opts.dotall)
                    .case_insensitive(opts.case_insensitive)
                    .unicode(!opts.ascii_mode)
                    .build()
                    .ok();
            }
        }
    } else {
        return Err(OperatorCreationError::new(
            "the regex operator needs a regular expression as an argument",
            arg_idx,
        ));
    };

    Ok(OpRegex {
        regex,
        text_only_regex,
        capture_group_names: Default::default(),
        output_group_id,
        opts,
    })
}

pub fn create_op_regex(
    value: &str,
    opts: RegexOptions,
) -> Result<OperatorData, OperatorCreationError> {
    Ok(OperatorData::Regex(parse_op_regex(
        Some(value.as_bytes().as_bstr()),
        None,
        opts,
    )?))
}

pub fn create_op_regex_lines() -> OperatorData {
    OperatorData::Regex(
        parse_op_regex(
            Some("(?<>.+)\r?\n".as_bytes().as_bstr()),
            None,
            RegexOptions {
                ascii_mode: true,
                multimatch: true,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

pub fn setup_op_regex(
    string_store: &mut StringStore,
    op: &mut OpRegex,
) -> Result<(), OperatorSetupError> {
    let mut unnamed_capture_groups: usize = 0;

    op.capture_group_names
        .extend(
            op.regex
                .capture_names()
                .enumerate()
                .into_iter()
                .map(|(i, name)| match name {
                    Some(name) => {
                        if i == op.output_group_id {
                            None
                        } else {
                            Some(string_store.intern_cloned(name))
                        }
                    }
                    None => {
                        let id = string_store.intern_moved(unnamed_capture_groups.to_string());
                        unnamed_capture_groups += 1;
                        Some(id)
                    }
                }),
        );
    Ok(())
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
        .map(|name| sess.entry_data.add_field(ms_id, *name))
        .collect();
    cgfs.sort_unstable();
    let output_field = cgfs[op.output_group_id];
    let re = TfRegex {
        regex: op.regex.clone(),
        text_only_regex: op
            .text_only_regex
            .as_ref()
            .map(|r| (r.clone(), r.capture_locations())),
        capture_group_fields: cgfs,
        capture_locs: op.regex.capture_locations(),
        multimatch: op.opts.multimatch,
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
    fn captures_read_at(&mut self, start: usize) -> Option<(usize, usize)> {
        match self {
            AnyRegex::Text(re, cl, input) => re
                .captures_read_at(cl, input, start)
                .map(|m| (m.start(), m.end())),
            AnyRegex::Bytes(re, cl, input) => re
                .captures_read_at(cl, input, start)
                .map(|m| (m.start(), m.end())),
        }
    }
    fn next_after_empty(&mut self, end: usize) -> usize {
        match self {
            AnyRegex::Text(_re, _cl, input) => {
                let mut res = end + 1;
                // this is stupid. if this was nightly rust, we could use
                // ceil_char_boundary(end + 1) instead
                while !input.is_char_boundary(res) {
                    res += 1;
                }
                res
            }
            AnyRegex::Bytes(_re, _cl, _input) => end + 1,
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
    fn data(&mut self) -> &[u8] {
        match self {
            AnyRegex::Text(_re, _cl, input) => input.as_bytes(),
            AnyRegex::Bytes(_re, _cl, input) => input,
        }
    }
    fn next(&mut self, last_end: &mut Option<usize>, next_start: &mut usize) -> bool {
        if *next_start > self.data().len() {
            return false;
        }
        let (s, e) = match self.captures_read_at(*next_start) {
            None => return false,
            Some((s, e)) => (s, e),
        };
        if s == e {
            *next_start = self.next_after_empty(e);
            if Some(e) == *last_end {
                return self.next(last_end, next_start);
            }
        } else {
            *next_start = e;
        }
        *last_end = Some(e);
        true
    }
}

fn match_regex_inner<'a, 'b, 'c>(
    input_field_id: FieldId,
    run_length: RunLength,
    offset: usize,
    mut regex: AnyRegex<'a, 'b>,
    capture_group_fields: &Vec<FieldId>,
    multimatch: bool,
    rbs: &mut RegexBatchState,
    fields: &Universe<NonMaxUsize, RefCell<Field>>,
    command_buffer: &mut FDCommandBuffer,
) {
    let mut last_end = None;
    let mut next_start = 0;
    let mut match_count: RunLength = 0;
    let starting_field_idx = rbs.field_idx;
    while regex.next(&mut last_end, &mut next_start) {
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
                        begin: offset + cg_begin,
                        end: offset + cg_end,
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
        command_buffer.push_action(FieldActionKind::Dup, starting_field_idx, match_count - 1);
    }
    rbs.match_count += match_count as usize;
}

pub fn handle_tf_regex_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field_id) = sess.claim_batch(tf_id, &re.capture_group_fields);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id;
    let command_buffer = &mut sess.entry_data.match_sets[tf.match_set_id].command_buffer;
    command_buffer.begin_action_set(tf.ordering_id.into());
    let input_field = sess.entry_data.fields[input_field_id].borrow_mut();
    let mut fd_ref_iter = FDRefIterLazy::default();
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
                for (v, rl) in InlineTextIter::from_typed_range(&range, text) {
                    let any_regex = if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                        AnyRegex::Text(regex, capture_locs, v)
                    } else {
                        AnyRegex::Bytes(&mut re.regex, &mut re.capture_locs, v.as_bytes())
                    };
                    match_regex_inner(
                        input_field_id,
                        rl,
                        0,
                        any_regex,
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.entry_data.fields,
                        command_buffer,
                    );
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (v, rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    match_regex_inner(
                        input_field_id,
                        rl,
                        0,
                        AnyRegex::Bytes(&mut re.regex, &mut re.capture_locs, v),
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.entry_data.fields,
                        command_buffer,
                    );
                }
            }
            FDTypedSlice::Reference(refs) => {
                for fr in fd_ref_iter.get_iter(&sess.entry_data.fields, rbs.field_idx, &range, refs)
                {
                    let any_regex = match fr.data {
                        FDTypedValue::StreamValueId(_) => todo!(),
                        FDTypedValue::BytesInline(v) => AnyRegex::Bytes(
                            &mut re.regex,
                            &mut re.capture_locs,
                            &v.as_bytes()[fr.begin..fr.end],
                        ),
                        FDTypedValue::TextInline(v) => {
                            if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                                AnyRegex::Text(regex, capture_locs, &v[fr.begin..fr.end])
                            } else {
                                AnyRegex::Bytes(
                                    &mut re.regex,
                                    &mut re.capture_locs,
                                    &v.as_bytes()[fr.begin..fr.end],
                                )
                            }
                        }
                        _ => panic!("invalid target type for FieldReference"),
                    };
                    match_regex_inner(
                        fr.field,
                        fr.run_len,
                        fr.begin,
                        any_regex,
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.entry_data.fields,
                        command_buffer,
                    );
                }
            }
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
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
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, rbs.match_count);
}
