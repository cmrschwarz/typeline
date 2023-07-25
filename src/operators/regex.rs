use arrayvec::ArrayString;

use regex::{self, bytes};
use smallstr::SmallString;
use std::{borrow::Cow, cell::RefCell};

use std::num::NonZeroUsize;

use crate::{
    field_data::{
        command_buffer::{
            ActionProducingFieldIndex, CommandBuffer, FieldActionKind,
        },
        field_value_flags,
        iter_hall::{IterHall, IterId},
        iters::FieldIterator,
        push_interface::PushInterface,
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
        FieldReference, FieldValueKind, RunLength,
    },
    job_session::{Field, FieldId, JobData},
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
        RefAwareInlineTextIter, RefAwareStreamValueIter,
    },
    stream_value::{StreamValueData, StreamValueId},
    utils::{
        self, i64_to_str,
        string_store::{StringStore, StringStoreEntry},
        universe::Universe,
        USIZE_MAX_DECIMAL_DIGITS,
    },
};
use bstr::ByteSlice;

use super::{
    errors::{
        OperatorApplicationError, OperatorCreationError, OperatorSetupError,
    },
    operator::{OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Clone)]
pub struct OpRegex {
    pub text_only_regex: Option<regex::Regex>,
    pub regex: bytes::Regex,
    pub opts: RegexOptions,
    pub output_group_id: usize,
    pub capture_group_names: Vec<Option<StringStoreEntry>>,
}

pub struct TfRegex {
    regex: bytes::Regex,
    capture_locs: bytes::CaptureLocations,
    text_only_regex: Option<(regex::Regex, regex::CaptureLocations)>,
    capture_group_fields: Vec<Option<FieldId>>,
    input_field_iter_id: IterId,
    next_start: usize,
    apf_idx: ActionProducingFieldIndex,
    multimatch: bool,
    non_mandatory: bool,
    allow_overlapping: bool,
}

#[derive(Clone, Default)]
pub struct RegexOptions {
    // disable unicode for character classes making e.g. \w  only
    // match ascii characters
    // for byte sequences, having this disabled means that '.' will match
    // any byte sequence that looks like a valid UTF-8 character
    pub ascii_mode: bool,

    // don't attempt to preserve strings as valid utf8 and turn everything into
    // byte sequences
    // !!! if used from the cli, this implies ascii mode unless 'u' is specified
    pub binary_mode: bool,

    // return multiple matches instead of only the first
    pub multimatch: bool,

    // allow overlapping matches (only meaningful if multimatch is enabled)
    pub overlapping: bool,

    // produce null values for all capture groups instead of dropping the record
    // if matching fails
    pub non_mandatory: bool,

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
        if self.opts.ascii_mode && !self.opts.binary_mode {
            res.push('a');
        }
        if self.opts.binary_mode {
            res.push('b');
        }
        if self.opts.dotall {
            res.push('d');
        }
        if self.opts.case_insensitive {
            res.push('i');
        }
        if self.opts.line_based {
            res.push('l');
        }
        if self.opts.multimatch {
            res.push('m');
        }
        if self.opts.non_mandatory {
            res.push('n');
        }
        if self.opts.overlapping {
            res.push('o');
        }
        if self.opts.binary_mode && !self.opts.ascii_mode {
            res.push('u');
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
                empty_group_replacement_str.push_str(&utils::usize_to_str(
                    rand::random::<NonZeroUsize>().get(),
                ));
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
                        // make sure to display the error based on the original regex,
                        // not the tampered one
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
    return Ok((re, opt_empty_group_replacement_str));
}

pub fn parse_op_regex(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
    opts: RegexOptions,
) -> Result<OpRegex, OperatorCreationError> {
    let regex;
    let text_only_regex;
    let mut output_group_id = 0;
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new(
                "missing argument for the regex operator",
                arg_idx,
            )
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "regex pattern must be legal UTF-8",
                arg_idx,
            )
        })?;

    let (re, empty_group_replacement) = preparse_replace_empty_capture_group(
        value_str, &opts,
    )
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
    text_only_regex = if !opts.binary_mode {
        regex::RegexBuilder::new(&re)
            .multi_line(opts.line_based)
            .dot_matches_new_line(opts.dotall)
            .case_insensitive(opts.case_insensitive)
            .unicode(!opts.ascii_mode)
            .build()
            .ok()
    } else {
        None
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
        Some(value.as_bytes()),
        None,
        opts,
    )?))
}

pub fn create_op_regex_lines() -> OperatorData {
    OperatorData::Regex(
        parse_op_regex(
            Some("(?<>.+)\r?\n".as_bytes()),
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

    op.capture_group_names.extend(
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
                    unnamed_capture_groups += 1;
                    if i == 0 {
                        None
                    } else {
                        let id = string_store
                            .intern_moved(unnamed_capture_groups.to_string());
                        Some(id)
                    }
                }
            }),
    );
    Ok(())
}

pub fn setup_tf_regex<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpRegex,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let cb = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
        .command_buffer;
    let apf_idx = cb.claim_apf(tf_state.ordering_id);
    let apf_succ = cb.peek_next_apf_id(); // this will always end up being valid because of the terminator
    sess.field_mgr
        .register_field_reference(tf_state.output_field, tf_state.input_field);
    let mut output_field =
        sess.field_mgr.fields[tf_state.output_field].borrow_mut();

    output_field.min_apf_idx = Some(apf_succ);
    drop(output_field);
    let cgfs: Vec<Option<FieldId>> = op
        .capture_group_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            if i == op.output_group_id {
                Some(tf_state.output_field)
            } else if let Some(name) = name {
                let field_id = sess
                    .field_mgr
                    .add_field(tf_state.match_set_id, Some(apf_succ));
                sess.match_set_mgr.add_field_name(
                    &sess.field_mgr,
                    field_id,
                    *name,
                );
                Some(field_id)
            } else {
                None
            }
        })
        .collect();
    tf_state.preferred_input_type = Some(FieldValueKind::BytesInline);

    TransformData::Regex(TfRegex {
        regex: op.regex.clone(),
        text_only_regex: op
            .text_only_regex
            .as_ref()
            .map(|r| (r.clone(), r.capture_locations())),
        capture_group_fields: cgfs,
        capture_locs: op.regex.capture_locations(),
        multimatch: op.opts.multimatch,
        non_mandatory: op.opts.non_mandatory,
        allow_overlapping: op.opts.overlapping,
        input_field_iter_id: sess.field_mgr.fields[tf_state.input_field]
            .borrow_mut()
            .field_data
            .claim_iter(),
        next_start: 0,
        apf_idx,
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
    fn push(
        &self,
        fd: &mut IterHall,
        data: &Self::Data,
        start: usize,
        end: usize,
        rl: usize,
    );
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
    fn push(
        &self,
        fd: &mut IterHall,
        data: &Self::Data,
        start: usize,
        end: usize,
        rl: usize,
    ) {
        fd.push_str(&data[start..end], rl, true, false)
    }

    fn data_len(data: &Self::Data) -> usize {
        data.len()
    }
    fn allows_overlapping(&self) -> bool {
        self.allow_overlapping
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
    fn push(
        &self,
        fd: &mut IterHall,
        data: &Self::Data,
        start: usize,
        end: usize,
        rl: usize,
    ) {
        fd.push_bytes(&data[start..end], rl, true, false)
    }
    fn data_len(data: &Self::Data) -> usize {
        data.len()
    }
    fn allows_overlapping(&self) -> bool {
        self.allow_overlapping
    }
}

#[inline(always)]
fn match_regex_inner<'a, 'b, const PUSH_REF: bool, R: AnyRegex>(
    rmis: &mut RegexMatchInnerState<'a, 'b>,
    regex: &mut R,
    data: &R::Data,
    run_length: RunLength,
    offset: usize,
) -> bool {
    if rmis.batch_state.field_pos_output
        == rmis.batch_state.batch_end_field_pos_output
    {
        return true;
    }
    let mut match_count: usize = 0;
    let has_previous_matches = rmis.batch_state.next_start != 0;
    let rl = run_length as usize;
    let mut bse = false;
    while regex.next(data, &mut rmis.batch_state.next_start) {
        match_count += 1;
        for c in 0..regex.captures_locs_len() {
            if let Some(field_id) = rmis.batch_state.capture_group_fields[c] {
                let field = &mut rmis.batch_state.fields[field_id]
                    .borrow_mut()
                    .field_data;
                if let Some((cg_begin, cg_end)) = regex.captures_locs_get(c) {
                    if PUSH_REF {
                        field.push_reference(
                            FieldReference {
                                field: rmis.source_field,
                                begin: offset + cg_begin,
                                end: offset + cg_end,
                            },
                            rl,
                            true,
                            true,
                        );
                    } else {
                        regex.push(field, data, cg_begin, cg_end, rl);
                    }
                } else {
                    field.push_null(run_length as usize, true);
                }
            }
        }
        if !rmis.batch_state.multimatch {
            break;
        }
        if rmis.batch_state.field_pos_output + match_count
            == rmis.batch_state.batch_end_field_pos_output
        {
            bse = true;
            break;
        }
    }

    if has_previous_matches {
        rmis.command_buffer.push_action_with_usize_rl(
            rmis.batch_state.apf_idx,
            FieldActionKind::Dup,
            rmis.batch_state.field_pos_output,
            match_count,
        );
    } else {
        if match_count > 1 {
            rmis.command_buffer.push_action_with_usize_rl(
                rmis.batch_state.apf_idx,
                FieldActionKind::Dup,
                rmis.batch_state.field_pos_output,
                match_count - 1,
            );
        } else if match_count == 0 {
            if rmis.batch_state.non_mandatory {
                for c in 0..regex.captures_locs_len() {
                    if let Some(field_id) =
                        rmis.batch_state.capture_group_fields[c]
                    {
                        let field = &mut rmis.batch_state.fields[field_id]
                            .borrow_mut()
                            .field_data;
                        field.push_null(rl, true);
                    }
                }
                match_count = 1;
            } else {
                rmis.command_buffer.push_action(
                    rmis.batch_state.apf_idx,
                    FieldActionKind::Drop,
                    rmis.batch_state.field_pos_output,
                    1,
                );
            }
        }
    }
    if !bse {
        rmis.batch_state.next_start = 0;
    }
    rmis.batch_state.field_pos_output += match_count as usize;
    if !bse {
        rmis.batch_state.field_pos_input += run_length as usize;
    }
    return bse;
}

struct RegexBatchState<'a> {
    apf_idx: ActionProducingFieldIndex,
    field_pos_input: usize,
    field_pos_output: usize,
    batch_end_field_pos_output: usize,
    multimatch: bool,
    non_mandatory: bool,
    next_start: usize,
    capture_group_fields: &'a Vec<Option<FieldId>>,
    fields: &'a Universe<FieldId, RefCell<Field>>,
}

struct RegexMatchInnerState<'a, 'b> {
    batch_state: &'b mut RegexBatchState<'a>,
    command_buffer: &'b mut CommandBuffer,
    source_field: FieldId,
}

pub fn handle_tf_regex(
    sess: &mut JobData,
    tf_id: TransformId,
    re: &mut TfRegex,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    sess.tf_mgr.prepare_for_output(
        &sess.field_mgr,
        &mut sess.match_set_mgr,
        tf_id,
        re.capture_group_fields.iter().filter_map(|x| *x),
    );
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let op_id = tf.op_id.unwrap();

    sess.match_set_mgr.match_sets[tf.match_set_id]
        .command_buffer
        .begin_action_list(re.apf_idx);
    let input_field = sess
        .field_mgr
        .borrow_field_cow(input_field_id, tf.has_unconsumed_input());
    let iter_base = sess
        .field_mgr
        .get_iter_cow_aware(
            input_field_id,
            &input_field,
            re.input_field_iter_id,
        )
        .bounded(0, batch_size);
    let field_pos_start = iter_base.get_next_field_pos();
    let mut rbs = RegexBatchState {
        batch_end_field_pos_output: field_pos_start + tf.desired_batch_size,
        field_pos_input: field_pos_start,
        field_pos_output: field_pos_start,
        multimatch: re.multimatch,
        non_mandatory: re.non_mandatory,
        capture_group_fields: &re.capture_group_fields,
        fields: &sess.field_mgr.fields,
        next_start: re.next_start,
        apf_idx: re.apf_idx,
    };

    let mut iter =
        AutoDerefIter::new(&sess.field_mgr, input_field_id, iter_base);

    let mut text_regex =
        re.text_only_regex
            .as_mut()
            .map(|(regex, capture_locs)| TextRegex {
                re: regex,
                cl: capture_locs,
                allow_overlapping: re.allow_overlapping,
            });
    let mut bytes_regex = BytesRegex {
        re: &mut re.regex,
        cl: &mut re.capture_locs,
        allow_overlapping: re.allow_overlapping,
    };

    let mut bse = false; // 'batch size exceeded'
    let mut hit_stream_val = false;
    'batch: while let Some(range) = iter.typed_range_fwd(
        &mut sess.match_set_mgr,
        usize::MAX,
        field_value_flags::BYTES_ARE_UTF8,
    ) {
        let mut rmis = RegexMatchInnerState {
            batch_state: &mut rbs,
            command_buffer: &mut sess.match_set_mgr.match_sets
                [tf.match_set_id]
                .command_buffer,
            source_field: range.field_id,
        };
        match range.base.data {
            TypedSlice::TextInline(text) => {
                for (v, rl, offset) in
                    RefAwareInlineTextIter::from_range(&range, text)
                {
                    if let Some(tr) = &mut text_regex {
                        bse = match_regex_inner::<true, _>(
                            &mut rmis, tr, v, rl, offset,
                        );
                    } else {
                        bse = match_regex_inner::<true, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            v.as_bytes(),
                            rl,
                            offset,
                        );
                    };
                    if bse {
                        break 'batch;
                    }
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, offset) in
                    RefAwareInlineBytesIter::from_range(&range, bytes)
                {
                    bse = match_regex_inner::<true, _>(
                        &mut rmis,
                        &mut bytes_regex,
                        v,
                        rl,
                        offset,
                    );
                    if bse {
                        break 'batch;
                    }
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, offset) in
                    RefAwareBytesBufferIter::from_range(&range, bytes)
                {
                    bse = match_regex_inner::<true, _>(
                        &mut rmis,
                        &mut bytes_regex,
                        v,
                        rl,
                        offset,
                    );
                    if bse {
                        break 'batch;
                    }
                }
            }
            TypedSlice::Integer(ints) => {
                if let Some(tr) = &mut text_regex {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, ints)
                    {
                        bse = match_regex_inner::<false, _>(
                            &mut rmis,
                            tr,
                            &i64_to_str(false, *v),
                            rl,
                            0,
                        );
                        if bse {
                            break 'batch;
                        }
                    }
                } else {
                    for (v, rl) in
                        TypedSliceIter::from_range(&range.base, ints)
                    {
                        bse = match_regex_inner::<false, _>(
                            &mut rmis,
                            &mut bytes_regex,
                            i64_to_str(false, *v).as_bytes(),
                            rl,
                            0,
                        );
                        if bse {
                            break 'batch;
                        }
                    }
                };
            }
            TypedSlice::StreamValueId(svs) => {
                for (v, offsets, rl) in
                    RefAwareStreamValueIter::from_range(&range, svs)
                {
                    let sv = &mut sess.sv_mgr.stream_values[v];
                    if sv.done {
                        let data;
                        match &sv.data {
                            StreamValueData::Dropped => unreachable!(),
                            StreamValueData::Error(e) => {
                                for cgi in re
                                    .capture_group_fields
                                    .iter()
                                    .filter_map(|v| *v)
                                {
                                    sess.field_mgr.fields[cgi]
                                        .borrow_mut()
                                        .field_data
                                        .push_error(
                                            e.clone(),
                                            rl as usize,
                                            true,
                                            false,
                                        );
                                }
                                continue;
                            }
                            StreamValueData::Bytes(b) => {
                                data = &b[offsets.unwrap_or(0..b.len())];
                            }
                        }
                        bse = if let Some(tr) = &mut text_regex {
                            if sv.bytes_are_utf8 {
                                match_regex_inner::<true, _>(
                                    &mut rmis,
                                    tr,
                                    unsafe {
                                        std::str::from_utf8_unchecked(data)
                                    },
                                    rl,
                                    0,
                                )
                            } else {
                                match_regex_inner::<true, _>(
                                    &mut rmis,
                                    &mut bytes_regex,
                                    data,
                                    rl,
                                    0,
                                )
                            }
                        } else {
                            match_regex_inner::<true, _>(
                                &mut rmis,
                                &mut bytes_regex,
                                data,
                                rl,
                                0,
                            )
                        }
                    } else {
                        sv.promote_to_buffer();
                        sv.subscribe(tf_id, rl as usize, true);
                        // TODO: if multimatch is false and we are in optional mode
                        // we can theoretically continue here, because there will always be exactly one match
                        // we would need StreamValueData to support null for that though
                        hit_stream_val = true;
                        break 'batch;
                    }
                    if bse {
                        break 'batch;
                    }
                }
            }
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::Null(_)
            | TypedSlice::Success(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::Object(_) => {
                rbs.field_pos_input += range.base.field_count;
                rbs.field_pos_output += range.base.field_count;
                for cgi in re.capture_group_fields.iter().filter_map(|v| *v) {
                    sess.field_mgr.fields[cgi]
                        .borrow_mut()
                        .field_data
                        .push_error(
                            OperatorApplicationError::new(
                                "regex type error",
                                op_id,
                            ),
                            range.base.field_count,
                            true,
                            true,
                        );
                }
            }
        }
    }
    sess.match_set_mgr.match_sets[tf.match_set_id]
        .command_buffer
        .end_action_list(re.apf_idx);
    re.next_start = rbs.next_start;
    let mut base_iter = iter.into_base_iter();

    let produced_records = rbs.field_pos_output - field_pos_start;
    if bse || hit_stream_val {
        let unclaimed_batch_size =
            batch_size - (rbs.field_pos_input - field_pos_start);
        sess.tf_mgr.unclaim_batch_size(tf_id, unclaimed_batch_size);
    }
    if !bse && !hit_stream_val {
        if input_done {
            drop(input_field);
            sess.unlink_transform(tf_id, produced_records);
            return;
        }
    }
    if bse || hit_stream_val {
        base_iter.move_to_field_pos(rbs.field_pos_input);
        if !hit_stream_val {
            sess.tf_mgr.push_tf_in_ready_queue(tf_id);
        }
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
    }
    sess.field_mgr.store_iter_cow_aware(
        input_field_id,
        &input_field,
        re.input_field_iter_id,
        base_iter,
    );
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, produced_records);
}

pub fn handle_tf_regex_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    _tf: &mut TfRegex,
    sv_id: StreamValueId,
    _custom: usize,
) {
    debug_assert!(sess.sv_mgr.stream_values[sv_id].done);
    sess.tf_mgr.push_tf_in_ready_queue(tf_id);
}

#[cfg(test)]
mod test {

    use super::{parse_op_regex, RegexOptions};

    #[test]
    fn empty_capture_group_does_not_mess_with_error_string() {
        let res = parse_op_regex(
            Some("?(<>)(".as_bytes()),
            None,
            RegexOptions::default(),
        );
        assert!(res.is_err_and(|e| {
            //TODO: improve this error message
            assert_eq!(e.message, "failed to compile regex: regex parse error:\n    ?(<>)(\n    ^\nerror: repetition operator missing expression");
            true
        }));
    }
}
