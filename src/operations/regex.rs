use arrayvec::ArrayString;
use bstr::{BStr, ByteSlice};
use nonmax::NonMaxUsize;
use regex::{self, bytes};
use smallstr::SmallString;
use std::borrow::Cow;
use std::cell::RefCell;
use std::ops::Deref;

use std::num::NonZeroUsize;

use crate::field_data::command_buffer::{CommandBuffer, FieldActionKind};
use crate::field_data::iter_hall::IterHall;
use crate::field_data::push_interface::PushInterface;
use crate::field_data::typed_iters::TypedSliceIter;
use crate::field_data::{field_value_flags, FieldValueKind, RunLength};
use crate::ref_iter::{
    AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
};
use crate::utils::universe::Universe;
use crate::utils::{self, i64_to_str, USIZE_MAX_DECIMAL_DIGITS};
use crate::worker_thread_session::Field;
use crate::{
    field_data::typed::TypedSlice,
    field_data::{iter_hall::IterId, iters::FieldIterator, FieldReference},
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData},
};

use super::errors::OperatorSetupError;
use super::operator::OperatorData;
use super::transform::TransformState;
use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    transform::{TransformData, TransformId},
};

pub struct OpRegex {
    regex: bytes::Regex,
    text_only_regex: Option<regex::Regex>,
    opts: RegexOptions,
    output_group_id: usize,
    capture_group_names: Vec<Option<StringStoreEntry>>,
}

pub struct TfRegex {
    regex: bytes::Regex,
    capture_locs: bytes::CaptureLocations,
    text_only_regex: Option<(regex::Regex, regex::CaptureLocations)>,
    capture_group_fields: Vec<FieldId>,
    input_field_iter_id: IterId,
    multimatch: bool,
}

struct RegexBatchState {
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

impl OpRegex {
    pub fn default_op_name(&self) -> SmallString<[u8; 16]> {
        let mut res = SmallString::from_str("r");
        if self.opts.ascii_mode {
            res.push('b');
        }
        if self.opts.multimatch {
            res.push('m');
        }
        if self.opts.line_based {
            res.push('l');
        }
        if self.opts.dotall {
            res.push('d');
        }
        if self.opts.case_insensitive {
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
                    .push_str(&utils::usize_to_str(rand::random::<NonZeroUsize>().get()));
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
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new("missing argument for the regex operator", arg_idx)
        })?
        .to_str()
        .map_err(|_| OperatorCreationError::new("regex pattern must be legal UTF-8", arg_idx))?;

    let (re, empty_group_replacement) = preparse_replace_empty_capture_group(value_str, &opts)
        .map_err(|e| {
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
            .find(|(_i, cn)| *cn == Some(&egr.as_str()))
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
    op: &'a OpRegex,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    let cgfs: Vec<FieldId> = op
        .capture_group_names
        .iter()
        .map(|name| sess.record_mgr.add_field(tf_state.match_set_id, *name))
        .collect();
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);
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
        input_field_iter_id: sess.record_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
    };
    sess.record_mgr
        .initialize_tf_output_fields(tf_state.ordering_id, &re.capture_group_fields);
    (TransformData::Regex(re), output_field)
}

struct TextRegex<'a> {
    re: &'a mut regex::Regex,
    cl: &'a mut regex::CaptureLocations,
    data: &'a str,
}

struct BytesRegex<'a> {
    re: &'a mut bytes::Regex,
    cl: &'a mut bytes::CaptureLocations,
    data: &'a [u8],
}

trait AnyRegex {
    fn captures_read_at(&mut self, start: usize) -> Option<(usize, usize)>;
    fn next_after_empty(&mut self, end: usize) -> usize;
    fn captures_locs_len(&mut self) -> usize;
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)>;
    fn data(&mut self) -> &[u8];
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
    fn push(&self, fd: &mut IterHall, start: usize, end: usize, rl: usize);
}

impl<'a> AnyRegex for TextRegex<'a> {
    fn captures_read_at(&mut self, start: usize) -> Option<(usize, usize)> {
        self.re
            .captures_read_at(self.cl, self.data, start)
            .map(|m| (m.start(), m.end()))
    }
    fn next_after_empty(&mut self, end: usize) -> usize {
        let mut res = end + 1;
        // this is stupid. if this was nightly rust, we could use
        // ceil_char_boundary(end + 1) instead
        while !self.data.is_char_boundary(res) {
            res += 1;
        }
        res
    }
    fn captures_locs_len(&mut self) -> usize {
        self.cl.len()
    }
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)> {
        self.cl.get(i)
    }
    fn data(&mut self) -> &[u8] {
        self.data.as_bytes()
    }
    fn push(&self, fd: &mut IterHall, start: usize, end: usize, rl: usize) {
        fd.push_str(&self.data[start..end], rl, true, false)
    }
}

impl<'a> AnyRegex for BytesRegex<'a> {
    fn captures_read_at(&mut self, start: usize) -> Option<(usize, usize)> {
        self.re
            .captures_read_at(self.cl, self.data, start)
            .map(|m| (m.start(), m.end()))
    }
    fn next_after_empty(&mut self, end: usize) -> usize {
        end + 1
    }
    fn captures_locs_len(&mut self) -> usize {
        self.cl.len()
    }
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)> {
        self.cl.get(i)
    }
    fn data(&mut self) -> &[u8] {
        self.data
    }
    fn push(&self, fd: &mut IterHall, start: usize, end: usize, rl: usize) {
        fd.push_bytes(&self.data[start..end], rl, true, false)
    }
}

fn match_regex_inner<'a, 'b, const PUSH_REF: bool>(
    input_field_id: FieldId,
    run_length: RunLength,
    offset: usize,
    mut regex: impl AnyRegex,
    capture_group_fields: &Vec<FieldId>,
    multimatch: bool,
    rbs: &mut RegexBatchState,
    fields: &Universe<NonMaxUsize, RefCell<Field>>,
    command_buffer: &mut CommandBuffer,
) {
    let mut last_end = None;
    let mut next_start = 0;
    let mut match_count: RunLength = 0;
    let starting_field_idx = rbs.field_idx;
    let rl = run_length as usize;
    while regex.next(&mut last_end, &mut next_start) {
        match_count += 1;
        for c in 0..regex.captures_locs_len() {
            let field = &mut fields[capture_group_fields[c]].borrow_mut().field_data;
            if let Some((cg_begin, cg_end)) = regex.captures_locs_get(c) {
                if PUSH_REF {
                    field.push_reference(
                        FieldReference {
                            field: input_field_id,
                            begin: offset + cg_begin,
                            end: offset + cg_end,
                        },
                        rl,
                        true,
                        true,
                    );
                } else {
                    regex.push(field, cg_begin, cg_end, rl);
                }
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
        command_buffer.push_action(FieldActionKind::Drop, rbs.field_idx, 1);
    } else if match_count > 1 {
        command_buffer.push_action(FieldActionKind::Dup, starting_field_idx, match_count - 1);
    }
    rbs.match_count += match_count as usize;
}

pub fn handle_tf_regex(sess: &mut JobData<'_>, tf_id: TransformId, re: &mut TfRegex) {
    let (batch, input_field_id) = sess.claim_batch(tf_id);
    sess.prepare_for_output(tf_id, &re.capture_group_fields);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id;

    sess.record_mgr.match_sets[tf.match_set_id]
        .command_buffer
        .begin_action_set(tf.ordering_id.into());
    let input_field = sess.record_mgr.fields[input_field_id].borrow();
    let iter_base = input_field
        .deref()
        .field_data
        .get_iter(re.input_field_iter_id)
        .bounded(0, batch);
    let mut rbs = RegexBatchState {
        field_idx: iter_base.get_next_field_pos(),
        match_count: 0,
    };
    let mut iter = AutoDerefIter::new(
        &sess.record_mgr.fields,
        &mut sess.record_mgr.match_sets,
        input_field_id,
        iter_base,
        None,
    );

    while let Some(range) = iter.typed_range_fwd(
        &mut sess.record_mgr.match_sets,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        let command_buffer = &mut sess.record_mgr.match_sets[tf.match_set_id].command_buffer;
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for (v, rl, offset) in RefAwareInlineTextIter::from_range(&range, text) {
                    if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                        match_regex_inner::<true>(
                            range.field_id,
                            rl,
                            offset,
                            TextRegex {
                                re: regex,
                                cl: capture_locs,
                                data: v,
                            },
                            &re.capture_group_fields,
                            re.multimatch,
                            &mut rbs,
                            &sess.record_mgr.fields,
                            command_buffer,
                        );
                    } else {
                        match_regex_inner::<true>(
                            range.field_id,
                            rl,
                            offset,
                            BytesRegex {
                                re: &mut re.regex,
                                cl: &mut re.capture_locs,
                                data: v.as_bytes(),
                            },
                            &re.capture_group_fields,
                            re.multimatch,
                            &mut rbs,
                            &sess.record_mgr.fields,
                            command_buffer,
                        );
                    };
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, offset) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    match_regex_inner::<true>(
                        range.field_id,
                        rl,
                        offset,
                        BytesRegex {
                            re: &mut re.regex,
                            cl: &mut re.capture_locs,
                            data: v,
                        },
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.record_mgr.fields,
                        command_buffer,
                    );
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, offset) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    match_regex_inner::<true>(
                        range.field_id,
                        rl,
                        offset,
                        BytesRegex {
                            re: &mut re.regex,
                            cl: &mut re.capture_locs,
                            data: v,
                        },
                        &re.capture_group_fields,
                        re.multimatch,
                        &mut rbs,
                        &sess.record_mgr.fields,
                        command_buffer,
                    );
                }
            }
            TypedSlice::Integer(ints) => {
                if let Some((regex, capture_locs)) = &mut re.text_only_regex {
                    for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                        let text = i64_to_str(false, *v);
                        match_regex_inner::<false>(
                            range.field_id,
                            rl,
                            0,
                            TextRegex {
                                re: regex,
                                cl: capture_locs,
                                data: text.as_str(),
                            },
                            &re.capture_group_fields,
                            re.multimatch,
                            &mut rbs,
                            &sess.record_mgr.fields,
                            command_buffer,
                        );
                    }
                } else {
                    for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                        let text = i64_to_str(false, *v);
                        match_regex_inner::<false>(
                            range.field_id,
                            rl,
                            0,
                            BytesRegex {
                                re: &mut re.regex,
                                cl: &mut re.capture_locs,
                                data: text.as_bytes(),
                            },
                            &re.capture_group_fields,
                            re.multimatch,
                            &mut rbs,
                            &sess.record_mgr.fields,
                            command_buffer,
                        );
                    }
                };
            }
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::Unset(_)
            | TypedSlice::Null(_)
            | TypedSlice::Success(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::StreamValueId(_)
            | TypedSlice::Object(_) => {
                rbs.field_idx += range.base.field_count;
                for cgi in &re.capture_group_fields {
                    sess.record_mgr.fields[*cgi]
                        .borrow_mut()
                        .field_data
                        .push_error(
                            OperatorApplicationError::new("regex type error", op_id),
                            range.base.field_count,
                            true,
                            true,
                        );
                }
            }
        }
    }
    input_field
        .field_data
        .store_iter(re.input_field_iter_id, iter.into_base_iter());
    drop(input_field);
    sess.tf_mgr.update_ready_state(tf_id);
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, rbs.match_count);
}
