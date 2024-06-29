use arrayvec::ArrayString;

use bstr::ByteSlice;
use regex::bytes;
use smallvec::SmallVec;
use std::{borrow::Cow, cell::RefMut};

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, ParsedArgValue, Span},
    context::SessionSetupData,
    job::JobData,
    liveness_analysis::OpOutputIdx,
    options::operator_base_options::OperatorBaseOptionsInterned,
    record_data::{
        action_buffer::{ActionBuffer, ActorId, ActorRef},
        field::{Field, FieldId, FieldRefOffset},
        field_action::FieldActionKind,
        field_data::{
            field_value_flags, FieldData, FieldValueRepr, RunLength,
        },
        field_value::{FieldReference, SlicedFieldReference},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        formattable::RealizedFormatKey,
        iter_hall::{IterId, IterKind},
        iters::FieldIterator,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RangeOffsets, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
        stream_value::{StorageAgnosticStreamValueDataRef, StreamValueId},
        varying_type_inserter::VaryingTypeInserter,
    },
    utils::{
        indexing_type::IndexingType,
        int_string_conversions::{
            i64_to_str, usize_to_str, USIZE_MAX_DECIMAL_DIGITS,
        },
        string_store::StringStoreEntry,
        text_write::{MaybeTextWriteFlaggedAdapter, TextWriteIoAdapter},
    },
};

use super::{
    errors::{
        OperatorApplicationError, OperatorCreationError, OperatorSetupError,
    },
    operator::{
        OperatorBase, OperatorData, OperatorDataId, OperatorId, OperatorName,
        OperatorOffsetInChain, PreboundOutputsMap,
    },
    transform::{
        DefaultTransformName, TransformData, TransformId, TransformState,
    },
    utils::buffer_stream_values::{
        buffer_remaining_stream_values_in_auto_deref_iter,
        buffer_remaining_stream_values_in_sv_iter,
    },
};

#[derive(Clone)]
pub struct OpRegex {
    pub text_only_regex: Option<regex::Regex>,
    pub regex: bytes::Regex,
    pub opts: RegexOptions,
    pub output_group_id: usize,
    pub capture_group_names: Vec<Option<StringStoreEntry>>,
}

pub struct TfRegex<'a> {
    pub(crate) op: &'a OpRegex,
    pub(crate) regex: bytes::Regex,
    pub(crate) capture_locs: bytes::CaptureLocations,
    pub(crate) text_only_regex:
        Option<(regex::Regex, regex::CaptureLocations)>,
    pub(crate) capture_group_fields: Vec<Option<FieldId>>,
    pub(crate) input_field_iter_id: IterId,
    pub(crate) unfinished_value_offset: usize,
    pub(crate) actor_id: ActorId,
    pub(crate) input_field_ref_offset: FieldRefOffset,
    pub(crate) streams_kept_alive: usize,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RegexOptions {
    // disable unicode for character classes making e.g. \w  only
    // match ascii characters
    // for byte sequences, having this disabled means that '.' will match
    // any byte sequence that looks like a valid UTF-8 character
    pub ascii_mode: bool,

    // don't attempt to preserve strings as valid utf8 and turn everything
    // into byte sequences
    // !!! if used from the cli, this implies ascii mode unless 'u' is
    // specified
    pub binary_mode: bool,

    // return multiple matches instead of only the first
    pub multimatch: bool,

    // allow overlapping matches (only meaningful if multimatch is enabled)
    pub overlapping: bool,

    // produce null values for all capture groups instead of dropping the
    // record if matching fails
    pub non_mandatory: bool,

    // makes ^ and $ match lines in addition to start / end of stream
    // (commonly called multiline)
    pub line_based: bool,

    // makes . match \n
    pub dotall: bool,

    // enables case insensitive matches
    pub case_insensitive: bool,

    // return the whole input, not just the matched portion
    pub full: bool,

    // If the regex matches, discard. otherwise return the full input
    // (incompatible with multimatch)
    pub invert_match: bool,
}

impl RegexOptions {
    pub fn to_tf_string(&self, prefix: &str, regex: &str) -> String {
        let mut res = String::from(prefix);
        if *self != RegexOptions::default() {
            res.push('-');
        }
        if self.ascii_mode && !self.binary_mode {
            res.push('a');
        }
        if self.binary_mode {
            res.push('b');
        }
        if self.dotall {
            res.push('d');
        }
        if self.full {
            res.push('f');
        }
        if self.case_insensitive {
            res.push('i');
        }
        if self.line_based {
            res.push('l');
        }
        if self.multimatch {
            res.push('m');
        }
        if self.non_mandatory {
            res.push('n');
        }
        if self.overlapping {
            res.push('o');
        }
        if self.binary_mode && !self.ascii_mode {
            res.push('u');
        }
        if self.invert_match {
            res.push('v');
        }
        res.push('=');
        res.push_str(regex);
        res
    }
}

impl OpRegex {
    pub fn debug_op_name(&self) -> OperatorName {
        self.opts
            .to_tf_string("regex", &self.regex.to_string())
            .into()
    }
}

impl TfRegex<'_> {
    pub fn display_name(&self) -> DefaultTransformName {
        self.op
            .opts
            .to_tf_string("regex", &self.regex.to_string())
            .into()
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
    let mut dummy_group_id = 10001;
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
                    .push_str(&usize_to_str(dummy_group_id));
                dummy_group_id += 1;
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
                        regex_syntax::ast::ErrorKind::GroupNameDuplicate {
                            original: og_span,
                        } => {
                            if og_span.start.offset
                                == empty_group_span.start.offset
                            {
                                span = *pe.span();
                                continue;
                            }
                            break;
                        }
                        // if there is a 'real' error in the regex,
                        // make sure to display the error based on the original
                        // regex, not the tampered one
                        _ => return Ok((Cow::Borrowed(regex_str), None)),
                    }
                }
                break;
            }
            opt_empty_group_replacement_str =
                Some(empty_group_replacement_str);
            re = Cow::Owned(owned);
        }
    }
    Ok((re, opt_empty_group_replacement_str))
}

pub fn build_op_regex(
    regex: &str,
    opts: RegexOptions,
    span: Span,
) -> Result<OperatorData, OperatorCreationError> {
    let mut output_group_id = 0;

    let (re, empty_group_replacement) =
        preparse_replace_empty_capture_group(regex, &opts)
            .map_err(|e| OperatorCreationError { span, message: e })?;

    if empty_group_replacement.is_some() && opts.full {
        return Err(OperatorCreationError::new(
            "-f is incompatible with an explicit output capture group",
            span,
        ));
    }

    let regex = bytes::RegexBuilder::new(&re)
        .multi_line(opts.line_based)
        .dot_matches_new_line(opts.dotall)
        .case_insensitive(opts.case_insensitive)
        .unicode(!opts.ascii_mode)
        .build()
        .map_err(|e| OperatorCreationError {
            message: Cow::Owned(format!("failed to compile regex: {e}")),
            span,
        })?;

    if let Some(egr) = empty_group_replacement {
        output_group_id = regex
            .capture_names()
            .enumerate()
            .find(|(_i, cn)| *cn == Some(egr.as_str()))
            .map(|(i, _cn)| i)
            .unwrap();
    }
    let text_only_regex = if opts.binary_mode {
        None
    } else {
        regex::RegexBuilder::new(&re)
            .multi_line(opts.line_based)
            .dot_matches_new_line(opts.dotall)
            .case_insensitive(opts.case_insensitive)
            .unicode(!opts.ascii_mode)
            .build()
            .ok()
    };

    Ok(OperatorData::Regex(OpRegex {
        regex,
        text_only_regex,
        capture_group_names: Vec::new(),
        output_group_id,
        opts,
    }))
}

pub fn parse_op_regex(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let mut opts = RegexOptions::default();
    let mut unicode_mode = false;
    let mut ascii_or_unicode_span = Span::Builtin;
    let mut invert_or_multimatch_span = Span::Builtin;
    let mut regex = None;
    for arg in expr.parsed_args_iter_with_bounded_positionals(1, 1) {
        let arg = arg?;
        match arg.value {
            ParsedArgValue::Flag(flag) => match flag {
                b"a" => {
                    opts.ascii_mode = true;
                    ascii_or_unicode_span = arg.span;
                }
                b"b" => {
                    opts.binary_mode = true;
                }
                b"d" => {
                    opts.dotall = true;
                }
                b"f" => {
                    opts.full = true;
                }
                b"i" => {
                    opts.case_insensitive = true;
                }
                b"l" => {
                    opts.line_based = true;
                }
                b"m" => {
                    opts.multimatch = true;
                    invert_or_multimatch_span = arg.span;
                }
                b"n" => {
                    opts.non_mandatory = true;
                }
                b"o" => {
                    opts.overlapping = true;
                    opts.multimatch = true;
                    invert_or_multimatch_span = arg.span;
                }
                b"u" => {
                    unicode_mode = true;
                    ascii_or_unicode_span = arg.span;
                }
                b"v" => {
                    opts.invert_match = true;
                    invert_or_multimatch_span = arg.span;
                }
                other => {
                    return Err(
                        expr.error_flag_value_unsupported(other, arg.span)
                    );
                }
            },
            ParsedArgValue::NamedArg { .. } => {
                return Err(expr.error_named_args_unsupported(arg.span))
            }
            ParsedArgValue::PositionalArg { value, .. } => {
                regex = Some(
                    value
                        .expect_plain(expr.op_name, arg.span)?
                        .to_str()
                        .map_err(|_| {
                            OperatorCreationError::new(
                                "regex must be valid utf-8",
                                arg.span,
                            )
                        })?,
                );
            }
        }
        if opts.ascii_mode && unicode_mode {
            return Err(OperatorCreationError::new(
                "[a]scii and [u]nicode are mutually exclusive",
                ascii_or_unicode_span,
            ));
        }
        if opts.invert_match && opts.multimatch {
            return Err(OperatorCreationError::new(
                "in[v]ert match and [m]ultimatch/[o]verlapping are mutually exclusive",
                invert_or_multimatch_span,
            ));
        }
        if opts.binary_mode && !unicode_mode {
            opts.ascii_mode = true;
        }
    }
    build_op_regex(regex.unwrap(), opts, expr.span)
}

pub fn create_op_regex_with_opts(
    regex: &str,
    opts: RegexOptions,
) -> Result<OperatorData, OperatorCreationError> {
    build_op_regex(regex, opts, Span::Generated)
}
pub fn create_op_regex(
    regex: &str,
) -> Result<OperatorData, OperatorCreationError> {
    build_op_regex(regex, RegexOptions::default(), Span::Generated)
}

pub fn create_op_regex_lines() -> OperatorData {
    build_op_regex(
        "^(?<>.*)$",
        RegexOptions {
            ascii_mode: true,
            multimatch: true,
            line_based: true,
            ..Default::default()
        },
        Span::Generated,
    )
    .unwrap()
}

pub fn create_op_regex_trim_trailing_newline() -> OperatorData {
    build_op_regex(
        "^(?<>.*?)\n?$",
        RegexOptions {
            dotall: true,
            ..Default::default()
        },
        Span::Generated,
    )
    .unwrap()
}

pub fn setup_op_regex(
    op: &mut OpRegex,
    sess: &mut SessionSetupData,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    op_base_opts_interned: OperatorBaseOptionsInterned,
    op_data_id: OperatorDataId,
) -> Result<OperatorId, OperatorSetupError> {
    let mut unnamed_capture_groups: usize = 0;

    op.capture_group_names
        .extend(op.regex.capture_names().enumerate().map(|(i, name)| {
            if let Some(name) = name {
                if i == op.output_group_id {
                    None
                } else {
                    Some(sess.string_store.intern_cloned(name))
                }
            } else {
                unnamed_capture_groups += 1;
                if i == 0 {
                    None
                } else {
                    let id = sess
                        .string_store
                        .intern_moved(unnamed_capture_groups.to_string());
                    Some(id)
                }
            }
        }));

    Ok(sess.add_op_from_offset_in_chain(
        chain_id,
        offset_in_chain,
        op_base_opts_interned,
        op_data_id,
    ))
}

pub fn build_tf_regex<'a>(
    jd: &mut JobData,
    op_base: &OperatorBase,
    op: &'a OpRegex,
    tf_state: &mut TransformState,
    prebound_outputs: &PreboundOutputsMap,
) -> TransformData<'a> {
    let mut ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer
        .borrow_mut();
    let actor_id = ab.add_actor();
    let next_actor_id = ActorRef::Unconfirmed(ab.peek_next_actor_id());
    drop(ab);
    let output_field = jd.field_mgr.fields[tf_state.output_field].borrow();

    output_field.first_actor.set(next_actor_id);
    drop(output_field);
    let mut input_field_ref_offset = FieldRefOffset::MAX;
    let cgfs: Vec<Option<FieldId>> = op
        .capture_group_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let field_id = if i == op.output_group_id {
                Some(tf_state.output_field)
            } else if let Some(name) = name {
                let field_id = if let Some(field_id) =
                    prebound_outputs.get(&OpOutputIdx::from_usize(
                        op_base.outputs_start.into_usize() + i,
                    )) {
                    debug_assert!(
                        jd.field_mgr.fields[*field_id].borrow().name
                            == Some(*name)
                    );
                    *field_id
                } else {
                    jd.field_mgr.add_field(
                        &mut jd.match_set_mgr,
                        tf_state.match_set_id,
                        Some(*name),
                        next_actor_id,
                    )
                };
                Some(field_id)
            } else {
                None
            };
            if let Some(id) = field_id {
                let fro = jd
                    .field_mgr
                    .register_field_reference(id, tf_state.input_field);
                // all output fields should have the same
                // field ref layout
                debug_assert!(
                    input_field_ref_offset == fro
                        || input_field_ref_offset == FieldRefOffset::MAX
                );
                input_field_ref_offset = fro;
            }
            field_id
        })
        .collect();

    TransformData::Regex(TfRegex {
        regex: op.regex.clone(),
        text_only_regex: op
            .text_only_regex
            .as_ref()
            .map(|r| (r.clone(), r.capture_locations())),
        capture_group_fields: cgfs,
        capture_locs: op.regex.capture_locations(),
        input_field_iter_id: jd.field_mgr.claim_iter_non_cow(
            tf_state.input_field,
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
        ),
        // if we reach our target batch size while we are in the middle
        // of matching the regex agains a value (typically happens in
        // multimatch mode), we stop and continue next time at this
        // offset
        unfinished_value_offset: 0,
        actor_id,
        input_field_ref_offset,
        streams_kept_alive: 0,
        op,
    })
}

struct TextRegex<'a> {
    allow_overlapping: bool,
    re: &'a mut regex::Regex,
    cl: &'a mut regex::CaptureLocations,
}

struct BytesRegex<'a> {
    allow_overlapping: bool,
    re: &'a mut bytes::Regex,
    cl: &'a mut bytes::CaptureLocations,
}

trait AnyRegex {
    type Data: ?Sized;
    fn captures_read_at(
        &mut self,
        data: &Self::Data,
        start: usize,
    ) -> Option<(usize, usize)>;
    // 'element' means either unicode character or byte
    fn next_element(&mut self, data: &Self::Data, end: usize) -> usize;
    fn captures_locs_len(&mut self) -> usize;
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)>;
    fn data_len(data: &Self::Data) -> usize;
    fn allows_overlapping(&self) -> bool;
    fn next(&mut self, data: &Self::Data, next_start: &mut usize) -> bool {
        if *next_start > Self::data_len(data) {
            return false;
        }
        let (s, e) = match self.captures_read_at(data, *next_start) {
            None => return false,
            Some((s, e)) => (s, e),
        };
        if s == e || self.allows_overlapping() {
            *next_start = self.next_element(data, s);
        } else {
            *next_start = e;
        }
        true
    }
    fn matches(&mut self, data: &Self::Data) -> bool;
    fn get_byte_slice(data: &Self::Data, start: usize, end: usize) -> &[u8];
    fn get_str_slice(
        data: &Self::Data,
        start: usize,
        end: usize,
    ) -> Option<&str>;
    fn as_bytes(data: &Self::Data) -> &[u8] {
        Self::get_byte_slice(data, 0, Self::data_len(data))
    }
    fn as_str(data: &Self::Data) -> Option<&str> {
        Self::get_str_slice(data, 0, Self::data_len(data))
    }
}

impl<'a> AnyRegex for TextRegex<'a> {
    type Data = str;
    fn captures_read_at(
        &mut self,
        data: &Self::Data,
        start: usize,
    ) -> Option<(usize, usize)> {
        self.re
            .captures_read_at(self.cl, data, start)
            .map(|m| (m.start(), m.end()))
    }
    fn next_element(&mut self, data: &Self::Data, end: usize) -> usize {
        let mut res = end + 1;

        // this is stupid. if this was nightly rust, we could use
        // ceil_char_boundary(end + 1) instead
        while res < data.len() && !data.is_char_boundary(res) {
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

    fn data_len(data: &Self::Data) -> usize {
        data.len()
    }
    fn allows_overlapping(&self) -> bool {
        self.allow_overlapping
    }

    fn get_byte_slice(data: &Self::Data, start: usize, end: usize) -> &[u8] {
        data[start..end].as_bytes()
    }

    fn get_str_slice(
        data: &Self::Data,
        start: usize,
        end: usize,
    ) -> Option<&str> {
        Some(&data[start..end])
    }

    fn matches(&mut self, data: &Self::Data) -> bool {
        self.re.is_match(data)
    }
}

impl<'a> AnyRegex for BytesRegex<'a> {
    type Data = [u8];
    fn captures_read_at(
        &mut self,
        data: &Self::Data,
        start: usize,
    ) -> Option<(usize, usize)> {
        self.re
            .captures_read_at(self.cl, data, start)
            .map(|m| (m.start(), m.end()))
    }
    fn next_element(&mut self, _data: &Self::Data, end: usize) -> usize {
        end + 1
    }
    fn captures_locs_len(&mut self) -> usize {
        self.cl.len()
    }
    fn captures_locs_get(&mut self, i: usize) -> Option<(usize, usize)> {
        self.cl.get(i)
    }
    fn data_len(data: &Self::Data) -> usize {
        data.len()
    }
    fn allows_overlapping(&self) -> bool {
        self.allow_overlapping
    }
    fn get_byte_slice(data: &Self::Data, start: usize, end: usize) -> &[u8] {
        &data[start..end]
    }
    fn get_str_slice(
        _data: &Self::Data,
        _start: usize,
        _end: usize,
    ) -> Option<&str> {
        None
    }
    fn matches(&mut self, data: &Self::Data) -> bool {
        self.re.is_match(data)
    }
}

#[inline(always)]
fn match_regex_inner<const PUSH_REF: bool, R: AnyRegex>(
    rmis: &mut RegexMatchInnerState<'_, '_, '_>,
    regex: &mut R,
    data: &R::Data,
    run_length: RunLength,
    offsets: RangeOffsets,
) {
    if rmis.batch_state.field_pos_output
        >= rmis.batch_state.batch_end_field_pos_output
    {
        debug_assert!(
            rmis.batch_state.field_pos_output
                == rmis.batch_state.batch_end_field_pos_output
        );
        return;
    }
    let mut match_count: usize = 0;
    let rl = run_length as usize;
    let mut batch_size_exceeded = false;

    if rmis.batch_state.invert_match {
        if let Some(inserter) = &mut rmis.batch_state.inserters
            [rmis.batch_state.main_output_capture_group_idx]
        {
            if !regex.matches(data) {
                push_full::<PUSH_REF, R>(
                    data,
                    inserter,
                    rl,
                    offsets,
                    rmis.input_field_ref_offset,
                );
                match_count = 1;
            }
        };
    } else {
        while regex.next(data, &mut rmis.batch_state.next_start) {
            match_count += 1;
            for cg_idx in 0..regex.captures_locs_len() {
                let Some(inserter) = &mut rmis.batch_state.inserters[cg_idx]
                else {
                    continue;
                };
                // no need to check whether the capture locations was matched
                // as -f is incompatible with (?<>..)
                if rmis.batch_state.full
                    && cg_idx == rmis.batch_state.main_output_capture_group_idx
                {
                    push_full::<PUSH_REF, R>(
                        data,
                        inserter,
                        rl,
                        offsets,
                        rmis.input_field_ref_offset,
                    );
                    continue;
                }
                let Some((cg_begin, cg_end)) = regex.captures_locs_get(cg_idx)
                else {
                    inserter.push_zst(FieldValueRepr::Null, 1, true);
                    continue;
                };
                if PUSH_REF {
                    inserter.push_fixed_size_type(
                        SlicedFieldReference {
                            field_ref_offset: rmis.input_field_ref_offset,
                            begin: offsets.from_begin + cg_begin,
                            end: offsets.from_begin + cg_end,
                        },
                        rl,
                        true,
                        false,
                    );
                    continue;
                }
                if let Some(v) = R::get_str_slice(data, cg_begin, cg_end) {
                    inserter.push_str(v, rl, true, false);
                    continue;
                }
                inserter.push_bytes(
                    R::get_byte_slice(data, cg_begin, cg_end),
                    rl,
                    true,
                    false,
                );
            }
            if !rmis.batch_state.multimatch {
                break;
            }
            if rmis.batch_state.field_pos_output + match_count
                == rmis.batch_state.batch_end_field_pos_output
            {
                batch_size_exceeded = true;
                break;
            }
        }
    }

    if batch_size_exceeded {
        rmis.action_buffer.push_action(
            FieldActionKind::Dup,
            rmis.batch_state.field_pos_output,
            match_count,
        );
    } else if match_count > 1 {
        rmis.action_buffer.push_action(
            FieldActionKind::Dup,
            rmis.batch_state.field_pos_output,
            match_count - 1,
        );
    } else if match_count == 0 {
        if rmis.batch_state.non_mandatory {
            for c in 0..regex.captures_locs_len() {
                if let Some(ins) = &mut rmis.batch_state.inserters[c] {
                    ins.push_zst(FieldValueRepr::Null, rl, true);
                }
            }
            match_count = 1;
        } else {
            rmis.action_buffer.push_action(
                FieldActionKind::Drop,
                rmis.batch_state.field_pos_output,
                1,
            );
        }
    }

    if !batch_size_exceeded {
        rmis.batch_state.next_start = 0;
        rmis.batch_state.field_pos_input += run_length as usize;
    }
    rmis.batch_state.field_pos_output += match_count;
}

fn push_full<const PUSH_REF: bool, R: AnyRegex>(
    data: &<R as AnyRegex>::Data,
    inserter: &mut VaryingTypeInserter<&mut FieldData>,
    rl: usize,
    offsets: RangeOffsets,
    input_field_ref_offset: FieldRefOffset,
) {
    if !PUSH_REF {
        if let Some(v) = R::as_str(data) {
            inserter.push_str(v, rl, true, false);
            return;
        }
        inserter.push_bytes(R::as_bytes(data), rl, true, false);
        return;
    }
    if offsets == RangeOffsets::default() {
        inserter.push_fixed_size_type(
            FieldReference {
                field_ref_offset: input_field_ref_offset,
            },
            rl,
            true,
            true,
        );
        return;
    }
    inserter.push_fixed_size_type(
        SlicedFieldReference {
            field_ref_offset: input_field_ref_offset,
            begin: offsets.from_begin,
            end: offsets.from_begin + R::data_len(data),
        },
        rl,
        true,
        true,
    );
}

struct RegexBatchState<'a> {
    main_output_capture_group_idx: usize,
    field_pos_input: usize,
    field_pos_output: usize,
    batch_end_field_pos_output: usize,
    multimatch: bool,
    non_mandatory: bool,
    full: bool,
    invert_match: bool,
    next_start: usize,
    inserters: SmallVec<[Option<VaryingTypeInserter<&'a mut FieldData>>; 4]>,
}

impl RegexBatchState<'_> {
    fn batch_size_reached(&self) -> bool {
        debug_assert!(
            self.field_pos_output <= self.batch_end_field_pos_output
        );
        self.field_pos_output == self.batch_end_field_pos_output
    }
    fn consume_fields(&mut self, mut count: usize) -> usize {
        if self.field_pos_output + count >= self.batch_end_field_pos_output {
            count = self.batch_end_field_pos_output - self.field_pos_output;
        }
        self.field_pos_input += count;
        self.field_pos_output += count;
        count
    }
}

struct RegexMatchInnerState<'a, 'b, 'c> {
    batch_state: &'b mut RegexBatchState<'a>,
    action_buffer: RefMut<'c, ActionBuffer>,
    input_field_ref_offset: FieldRefOffset,
}

pub fn handle_tf_regex(
    jd: &mut JobData,
    tf_id: TransformId,
    re: &mut TfRegex,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    jd.tf_mgr.prepare_for_output(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
        re.capture_group_fields.iter().filter_map(|x| *x),
    );
    let tf = &jd.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let op_id = tf.op_id.unwrap();

    let input_field = jd
        .field_mgr
        .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
    let iter_base = jd
        .field_mgr
        .lookup_iter(input_field_id, &input_field, re.input_field_iter_id)
        .bounded(0, batch_size);
    let field_pos_start = iter_base.get_next_field_pos();
    let mut output_fields = SmallVec::<[Option<RefMut<'_, Field>>; 4]>::new();
    let mut output_field_inserters =
        SmallVec::<[Option<VaryingTypeInserter<&'_ mut FieldData>>; 4]>::new();
    let f_mgr = &jd.field_mgr;
    for of in &re.capture_group_fields {
        output_fields.push(of.map(|f| f_mgr.fields[f].borrow_mut()));
    }
    for of in &mut output_fields {
        output_field_inserters.push(of.as_mut().map(|f| {
            let mut ins = f.iter_hall.varying_type_inserter();
            // PERF: this might waste a lot of space if we have many nulls
            ins.drop_and_reserve(
                batch_size,
                FieldValueRepr::SlicedFieldReference.to_format(),
            );
            ins
        }));
    }
    jd.match_set_mgr.match_sets[tf.match_set_id]
        .action_buffer
        .borrow_mut()
        .begin_action_group(re.actor_id);
    let mut rbs = RegexBatchState {
        main_output_capture_group_idx: re.op.output_group_id,
        full: re.op.opts.full,
        batch_end_field_pos_output: field_pos_start + tf.desired_batch_size,
        field_pos_input: field_pos_start,
        field_pos_output: field_pos_start,
        multimatch: re.op.opts.multimatch,
        invert_match: re.op.opts.invert_match,
        non_mandatory: re.op.opts.non_mandatory,
        next_start: re.unfinished_value_offset,
        inserters: output_field_inserters,
    };

    let mut iter =
        AutoDerefIter::new(&jd.field_mgr, input_field_id, iter_base);

    let mut text_regex =
        re.text_only_regex
            .as_mut()
            .map(|(regex, capture_locs)| TextRegex {
                re: regex,
                cl: capture_locs,
                allow_overlapping: re.op.opts.overlapping,
            });
    let mut bytes_regex = BytesRegex {
        re: &mut re.regex,
        cl: &mut re.capture_locs,
        allow_overlapping: re.op.opts.overlapping,
    };

    let mut hit_stream_val = false;

    // we have do this to preserve the output order for cases
    // like `scr str=foo dup r-m=. p`
    // PERF: there are better way to do this, e.g. copying the matches
    // from our own output instead of rematching
    let max_run_len = if rbs.multimatch { 1 } else { usize::MAX };
    'batch: loop {
        if rbs.batch_size_reached() {
            break;
        }
        let Some(range) = iter.typed_range_fwd(
            &jd.match_set_mgr,
            max_run_len,
            field_value_flags::DEFAULT,
        ) else {
            break;
        };
        let mut rmis = RegexMatchInnerState {
            batch_state: &mut rbs,
            action_buffer: jd.match_set_mgr.match_sets[tf.match_set_id]
                .action_buffer
                .borrow_mut(),
            input_field_ref_offset: range
                .field_ref_offset
                .unwrap_or(re.input_field_ref_offset),
        };
        match range.base.data {
            FieldValueSlice::TextInline(text) => {
                for (v, rl, offsets) in
                    RefAwareInlineTextIter::from_range(&range, text)
                {
                    if let Some(tr) = &mut text_regex {
                        match_regex_inner::<true, _>(
                            &mut rmis, tr, v, rl, offsets,
                        );
                    } else {
                        match_regex_inner::<true, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            v.as_bytes(),
                            rl,
                            offsets,
                        );
                    };
                    if rmis.batch_state.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
            FieldValueSlice::BytesInline(bytes) => {
                for (v, rl, offset) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    match_regex_inner::<true, _>(
                        &mut rmis,
                        &mut bytes_regex,
                        v,
                        rl,
                        offset,
                    );
                    if rmis.batch_state.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
            FieldValueSlice::TextBuffer(bytes) => {
                for (v, rl, offset) in
                    RefAwareTextBufferIter::from_range(&range, bytes)
                {
                    if let Some(tr) = &mut text_regex {
                        match_regex_inner::<true, _>(
                            &mut rmis, tr, v, rl, offset,
                        );
                    } else {
                        match_regex_inner::<true, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            v.as_bytes(),
                            rl,
                            offset,
                        );
                    };
                    if rmis.batch_state.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
            FieldValueSlice::BytesBuffer(bytes) => {
                for (v, rl, offset) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    match_regex_inner::<true, _>(
                        &mut rmis,
                        &mut bytes_regex,
                        v,
                        rl,
                        offset,
                    );
                    if rmis.batch_state.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
            FieldValueSlice::Custom(custom_types) => {
                for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                    &range,
                    custom_types,
                ) {
                    let prev_len = jd.temp_vec.len();
                    let mut w = MaybeTextWriteFlaggedAdapter::new(
                        TextWriteIoAdapter(&mut jd.temp_vec),
                    );
                    v.format_raw(&mut w, &RealizedFormatKey::default())
                        .expect("custom stringify failed");
                    let valid_utf8 = w.is_utf8();
                    let str = &jd.temp_vec[prev_len..jd.temp_vec.len()];

                    if let (Some(tr), true) = (&mut text_regex, valid_utf8) {
                        match_regex_inner::<true, _>(
                            &mut rmis,
                            tr,
                            unsafe { std::str::from_utf8_unchecked(str) },
                            rl,
                            RangeOffsets::default(),
                        );
                    } else {
                        match_regex_inner::<true, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            str,
                            rl,
                            RangeOffsets::default(),
                        );
                    };
                    jd.temp_vec.truncate(prev_len);
                    if rmis.batch_state.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
            FieldValueSlice::Int(ints) => {
                if let Some(tr) = &mut text_regex {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        match_regex_inner::<false, _>(
                            &mut rmis,
                            tr,
                            &i64_to_str(false, *v),
                            rl,
                            RangeOffsets::default(),
                        );
                        if rmis.batch_state.batch_size_reached() {
                            break 'batch;
                        }
                    }
                } else {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        match_regex_inner::<false, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            i64_to_str(false, *v).as_bytes(),
                            rl,
                            RangeOffsets::default(),
                        );
                        if rmis.batch_state.batch_size_reached() {
                            break 'batch;
                        }
                    }
                };
            }
            FieldValueSlice::StreamValueId(svs) => {
                let mut sv_iter = FieldValueRangeIter::from_range(&range, svs);
                while let Some((&sv_id, rl)) = sv_iter.next() {
                    let sv = &mut jd.sv_mgr.stream_values[sv_id];
                    if re.streams_kept_alive > 0 {
                        let rc_diff = (rl as usize)
                            .saturating_sub(re.streams_kept_alive);
                        sv.ref_count -= rc_diff;
                        re.streams_kept_alive -= rc_diff;
                    }
                    if !sv.done {
                        sv.make_contiguous();
                        sv.subscribe(sv_id, tf_id, rl as usize, true, false);
                        // PERF: if multimatch is false and we are in optional
                        // mode we can theoretically
                        // continue here, because there will always be exactly
                        // one match we would need
                        // StreamValueData to support null for that though
                        hit_stream_val = true;
                        if re.streams_kept_alive > 0 {
                            let rc_diff = (rl as usize)
                                .saturating_sub(re.streams_kept_alive);
                            sv.ref_count -= rc_diff;
                            re.streams_kept_alive -= rc_diff;
                        }
                        re.streams_kept_alive +=
                            buffer_remaining_stream_values_in_sv_iter(
                                &mut jd.sv_mgr,
                                sv_iter,
                                true,
                            );
                        drop(rmis);
                        re.streams_kept_alive +=
                            buffer_remaining_stream_values_in_auto_deref_iter(
                                &jd.match_set_mgr,
                                &mut jd.sv_mgr,
                                iter.clone(),
                                usize::MAX,
                                true,
                            );
                        break 'batch;
                    }
                    if let Some(e) = &sv.error {
                        for cgi in
                            rmis.batch_state.inserters.iter_mut().flatten()
                        {
                            cgi.push_error(
                                (**e).clone(),
                                rl as usize,
                                true,
                                false,
                            );
                        }
                        rmis.batch_state.field_pos_input += rl as usize;
                        rmis.batch_state.field_pos_output += rl as usize;
                        jd.sv_mgr.check_stream_value_ref_count(sv_id);
                        continue;
                    }
                    match &sv.single_data().as_ref().storage_agnostic() {
                        StorageAgnosticStreamValueDataRef::Bytes(b) => {
                            // PERF: maybe allow for stream values to be
                            // created to allow references or make the
                            // text/bytes type arc + range ...
                            match_regex_inner::<false, _>(
                                &mut rmis,
                                &mut bytes_regex,
                                b,
                                rl,
                                RangeOffsets::default(),
                            );
                            if rmis.batch_state.batch_size_reached() {
                                break 'batch;
                            }
                        }
                        StorageAgnosticStreamValueDataRef::Text(t) => {
                            if let Some(tr) = &mut text_regex {
                                match_regex_inner::<false, _>(
                                    &mut rmis,
                                    tr,
                                    t,
                                    rl,
                                    RangeOffsets::default(),
                                )
                            } else {
                                match_regex_inner::<false, _>(
                                    &mut rmis,
                                    &mut bytes_regex,
                                    t.as_bytes(),
                                    rl,
                                    RangeOffsets::default(),
                                )
                            };
                            if rmis.batch_state.batch_size_reached() {
                                break 'batch;
                            }
                        }
                    }
                    jd.sv_mgr.check_stream_value_ref_count(sv_id);
                }
            }
            FieldValueSlice::BigInt(_)
            | FieldValueSlice::Float(_)
            | FieldValueSlice::Rational(_) => {
                todo!();
            }
            FieldValueSlice::SlicedFieldReference(_)
            | FieldValueSlice::FieldReference(_) => unreachable!(),
            FieldValueSlice::Null(_)
            | FieldValueSlice::Undefined(_)
            | FieldValueSlice::Object(_)
            | FieldValueSlice::Array(_) => {
                let count = rbs.consume_fields(range.base.field_count);
                for inserter in rbs.inserters.iter_mut().flatten() {
                    inserter.push_fixed_size_type(
                        OperatorApplicationError::new_s(
                            format!(
                                "regex can't handle values of type `{}`",
                                range.base.data.repr()
                            ),
                            op_id,
                        ),
                        count,
                        true,
                        true,
                    );
                }
                if rbs.batch_size_reached() {
                    break 'batch;
                }
            }
            FieldValueSlice::Error(errs) => {
                for (e, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, errs)
                {
                    let count = rbs.consume_fields(rl as usize);
                    for inserter in rbs.inserters.iter_mut().flatten() {
                        inserter.push_fixed_size_type(
                            e.clone(),
                            count,
                            true,
                            true,
                        );
                    }
                    if rbs.batch_size_reached() {
                        break 'batch;
                    }
                }
            }
        }
    }
    re.unfinished_value_offset = rbs.next_start;
    let field_pos_input = rbs.field_pos_input; // brrwchck...
    let consumed_inputs = rbs.field_pos_input - field_pos_start;
    let produced_records = rbs.field_pos_output - field_pos_start;
    let bse = consumed_inputs < batch_size;
    jd.match_set_mgr.match_sets[tf.match_set_id]
        .action_buffer
        .borrow_mut()
        .end_action_group();
    let base_iter = iter.into_base_iter();
    if bse || hit_stream_val {
        let unclaimed_batch_size =
            batch_size - (rbs.field_pos_input - field_pos_start);
        jd.tf_mgr.unclaim_batch_size(tf_id, unclaimed_batch_size);
        drop(rbs);
        if !hit_stream_val {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
    } else if ps.next_batch_ready {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }

    if bse {
        // apply the action list first so we can move the iterator to the
        // correct continuation field
        // we explicitly don't store the iterator here so it stays at the
        // start position while we apply the action list
        drop(input_field);
        let input_field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, input_field_id);
        let mut iter = jd.field_mgr.lookup_iter(
            input_field_id,
            &input_field,
            re.input_field_iter_id,
        );
        let records = iter.next_n_fields(produced_records, true);
        debug_assert!(records == produced_records);
        jd.field_mgr
            .store_iter(input_field_id, re.input_field_iter_id, iter);
    } else {
        assert_eq!(base_iter.get_next_field_pos(), field_pos_input);
        jd.field_mgr.store_iter(
            input_field_id,
            re.input_field_iter_id,
            base_iter,
        );
    }
    jd.tf_mgr.submit_batch(
        tf_id,
        produced_records,
        ps.input_done && !hit_stream_val && !bse,
    );
}

pub fn handle_tf_regex_stream_value_update(
    jd: &mut JobData,
    tf_id: TransformId,
    _re: &mut TfRegex,
    sv_id: StreamValueId,
    _custom: usize,
) {
    debug_assert!(jd.sv_mgr.stream_values[sv_id].done);
    jd.tf_mgr.push_tf_in_ready_stack(tf_id);
}

#[cfg(test)]
mod test {

    use crate::{cli::call_expr::Span, operators::regex::build_op_regex};

    use super::RegexOptions;

    #[test]
    fn empty_capture_group_does_not_mess_with_error_string() {
        let res =
            build_op_regex("?(<>)(", RegexOptions::default(), Span::Generated);
        assert!(res.is_err_and(|e| {
            // ENHANCE: improve this error message
            assert_eq!(e.message, "failed to compile regex: regex parse error:\n    ?(<>)(\n    ^\nerror: repetition operator missing expression");
            true
        }));
    }
}
