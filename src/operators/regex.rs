use arrayvec::ArrayString;

use lazy_static::lazy_static;
use nonmax::NonMaxU16;
use regex::{self, bytes, Regex, RegexBuilder};
use smallstr::SmallString;
use smallvec::SmallVec;
use std::{borrow::Cow, cell::RefMut, collections::HashMap};

use std::num::NonZeroUsize;

use crate::{
    job_session::JobData,
    liveness_analysis::OpOutputIdx,
    options::argument::CliArgIdx,
    record_data::{
        command_buffer::{ActionBuffer, ActorId, ActorRef},
        field::{Field, FieldId, FieldIdOffset},
        field_action::FieldActionKind,
        field_data::{
            field_value_flags, FieldReference, FieldValueKind, RunLength,
        },
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::{PushInterface, VaryingTypeInserter},
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareStreamValueIter,
        },
        stream_value::{StreamValueData, StreamValueId},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
    },
    utils::{
        identity_hasher::BuildIdentityHasher,
        int_string_conversions::{
            i64_to_str, usize_to_str, USIZE_MAX_DECIMAL_DIGITS,
        },
        nonzero_ext::{nonmax_u16_wrapping_add, NonMaxU16Ext},
        string_store::{StringStore, StringStoreEntry},
    },
};
use bstr::ByteSlice;

use super::{
    errors::{
        OperatorApplicationError, OperatorCreationError, OperatorSetupError,
    },
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
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
    actor_id: ActorId,
    multimatch: bool,
    non_mandatory: bool,
    allow_overlapping: bool,
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
}

impl OpRegex {
    pub fn default_op_name(
        &self,
    ) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut res = SmallString::from_str("r");
        if self.opts != RegexOptions::default() {
            res.push('-');
        }
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
lazy_static! {
    static ref REGEX_CLI_ARG_REGEX: Regex =
        RegexBuilder::new("^(r|regex)(-((?<a>a)|(?<b>b)|(?<d>d)|(?<i>i)|(?<l>l)|(?<m>m)|(?<n>n)|(?<o>o)|(?<u>u))*)?$")
            .case_insensitive(true)
            .build()
            .unwrap();
}
pub fn try_match_regex_cli_argument(
    argname: &str,
    idx: Option<CliArgIdx>,
) -> Result<Option<RegexOptions>, OperatorCreationError> {
    if let Some(c) = REGEX_CLI_ARG_REGEX.captures(argname) {
        let mut opts = RegexOptions::default();
        let mut unicode_mode = false;
        if c.name("a").is_some() {
            opts.ascii_mode = true;
        }
        if c.name("b").is_some() {
            opts.binary_mode = true;
        }
        if c.name("d").is_some() {
            opts.dotall = true;
        }
        if c.name("i").is_some() {
            opts.case_insensitive = true;
        }
        if c.name("l").is_some() {
            opts.line_based = true;
        }
        if c.name("m").is_some() {
            opts.multimatch = true;
        }
        if c.name("n").is_some() {
            opts.non_mandatory = true;
        }
        if c.name("o").is_some() {
            opts.overlapping = true;
            opts.multimatch = true;
        }
        if c.name("u").is_some() {
            unicode_mode = true;
        }
        if opts.ascii_mode && unicode_mode {
            return Err(OperatorCreationError::new(
                "[a]scii and [u]nicode mode on regex are mutually exclusive",
                idx,
            ));
        }
        if opts.binary_mode && !unicode_mode {
            opts.ascii_mode = true;
        }
        Ok(Some(opts))
    } else {
        Ok(None)
    }
}

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
                empty_group_replacement_str.push_str(&usize_to_str(
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

pub fn parse_op_regex(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
    opts: RegexOptions,
) -> Result<OperatorData, OperatorCreationError> {
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
    .map_err(|e| OperatorCreationError {
        cli_arg_idx: arg_idx,
        message: e,
    })?;

    let regex = bytes::RegexBuilder::new(&re)
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
            .find(|(_i, cn)| *cn == Some(egr.as_str()))
            .map(|(i, _cn)| i)
            .unwrap();
    }
    let text_only_regex = if !opts.binary_mode {
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

    Ok(OperatorData::Regex(OpRegex {
        regex,
        text_only_regex,
        capture_group_names: Default::default(),
        output_group_id,
        opts,
    }))
}

pub fn create_op_regex_with_opts(
    regex: &str,
    opts: RegexOptions,
) -> Result<OperatorData, OperatorCreationError> {
    parse_op_regex(Some(regex.as_bytes()), None, opts)
}
pub fn create_op_regex(
    regex: &str,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_regex_with_opts(regex, Default::default())
}

pub fn create_op_regex_lines() -> OperatorData {
    parse_op_regex(
        Some("(?<>.+)\r?\n".as_bytes()),
        None,
        RegexOptions {
            ascii_mode: true,
            multimatch: true,
            ..Default::default()
        },
    )
    .unwrap()
}

pub fn setup_op_regex(
    string_store: &mut StringStore,
    op: &mut OpRegex,
) -> Result<(), OperatorSetupError> {
    let mut unnamed_capture_groups: usize = 0;

    op.capture_group_names
        .extend(op.regex.capture_names().enumerate().map(
            |(i, name)| match name {
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
            },
        ));
    Ok(())
}

pub fn build_tf_regex<'a>(
    sess: &mut JobData,
    op_base: &OperatorBase,
    op: &'a OpRegex,
    tf_state: &mut TransformState,
    prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
) -> TransformData<'a> {
    let cb = &mut sess.match_set_mgr.match_sets[tf_state.match_set_id]
        .action_buffer;
    let actor_id = cb.add_actor();
    let next_actor_id = ActorRef::Unconfirmed(cb.peek_next_actor_id());
    let mut output_field =
        sess.field_mgr.fields[tf_state.output_field].borrow_mut();

    output_field.first_actor = next_actor_id;
    drop(output_field);
    let cgfs: Vec<Option<FieldId>> = op
        .capture_group_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let field_id = if i == op.output_group_id {
                Some(tf_state.output_field)
            } else if let Some(name) = name {
                let field_id = if let Some(field_id) = prebound_outputs
                    .get(&(op_base.outputs_start + i as OpOutputIdx))
                {
                    *field_id
                } else {
                    sess.field_mgr
                        .add_field(tf_state.match_set_id, next_actor_id)
                };
                sess.match_set_mgr.set_field_name(
                    &sess.field_mgr,
                    field_id,
                    *name,
                );
                Some(field_id)
            } else {
                None
            };
            if let Some(id) = field_id {
                sess.field_mgr
                    .register_field_reference(id, tf_state.input_field);
            }
            field_id
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
            .iter_hall
            .claim_iter(),
        next_start: 0,
        actor_id,
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
    fn get_byte_slice(data: &Self::Data, start: usize, end: usize) -> &[u8];
    fn get_str_slice(
        data: &Self::Data,
        start: usize,
        end: usize,
    ) -> Option<&str>;
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
}

#[inline(always)]
fn match_regex_inner<const PUSH_REF: bool, R: AnyRegex>(
    rmis: &mut RegexMatchInnerState<'_, '_>,
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
    let rl = run_length as usize;
    let mut bse = false;
    while regex.next(data, &mut rmis.batch_state.next_start) {
        match_count += 1;
        for c in 0..regex.captures_locs_len() {
            if let Some(ins) = &mut rmis.batch_state.inserters[c] {
                if let Some((cg_begin, cg_end)) = regex.captures_locs_get(c) {
                    if PUSH_REF {
                        ins.push_field_reference(
                            FieldReference {
                                field_id_offset: rmis.field_ref_offset,
                                begin: offset + cg_begin,
                                end: offset + cg_end,
                            },
                            rl,
                        );
                    } else if let Some(v) =
                        R::get_str_slice(data, cg_begin, cg_end)
                    {
                        ins.push_inline_str(v, rl);
                    } else {
                        ins.push_inline_bytes(
                            R::get_byte_slice(data, cg_begin, cg_end),
                            rl,
                        );
                    }
                } else {
                    ins.push_null(1);
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

    if bse {
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
                    ins.push_null(rl);
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

    if !bse {
        rmis.batch_state.next_start = 0;
    }
    rmis.batch_state.field_pos_output += match_count;
    if !bse {
        rmis.batch_state.field_pos_input += run_length as usize;
    }
    bse
}

struct RegexBatchState<'a> {
    field_pos_input: usize,
    field_pos_output: usize,
    batch_end_field_pos_output: usize,
    multimatch: bool,
    non_mandatory: bool,
    next_start: usize,
    inserters: SmallVec<[Option<VaryingTypeInserter<'a>>; 4]>,
}

struct RegexMatchInnerState<'a, 'b> {
    batch_state: &'b mut RegexBatchState<'a>,
    action_buffer: &'b mut ActionBuffer,
    field_ref_offset: FieldIdOffset,
}

pub fn handle_tf_regex(
    sess: &mut JobData,
    tf_id: TransformId,
    re: &mut TfRegex,
) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    sess.tf_mgr.prepare_for_output(
        &mut sess.field_mgr,
        &mut sess.match_set_mgr,
        tf_id,
        re.capture_group_fields.iter().filter_map(|x| *x),
    );
    let tf = &sess.tf_mgr.transforms[tf_id];
    let input_field_id = tf.input_field;
    let op_id = tf.op_id.unwrap();

    let input_field = sess.field_mgr.get_cow_field_ref(
        &mut sess.match_set_mgr,
        input_field_id,
        tf.has_unconsumed_input(),
    );
    sess.match_set_mgr.match_sets[tf.match_set_id]
        .action_buffer
        .begin_action_group(re.actor_id);
    let iter_base = sess
        .field_mgr
        .lookup_iter(input_field_id, &input_field, re.input_field_iter_id)
        .bounded(0, batch_size);
    let field_pos_start = iter_base.get_next_field_pos();
    let mut output_fields = SmallVec::<[Option<RefMut<'_, Field>>; 4]>::new();
    let mut output_field_inserters =
        SmallVec::<[Option<VaryingTypeInserter<'_>>; 4]>::new();
    let f_mgr = &sess.field_mgr;
    for of in &re.capture_group_fields {
        output_fields.push(of.map(|f| f_mgr.fields[f].borrow_mut()));
    }
    let re_reserve_count =
        batch_size.min(RunLength::MAX as usize) as RunLength;
    for of in &mut output_fields {
        output_field_inserters.push(of.as_mut().map(|f| {
            let mut ins = f.iter_hall.varying_type_inserter(re_reserve_count);
            // PERF: this might waste a lot of space if we have many nulls
            ins.drop_and_reserve(
                batch_size,
                FieldValueKind::Reference,
                0,
                false,
            );
            ins
        }));
    }
    let mut rbs = RegexBatchState {
        batch_end_field_pos_output: field_pos_start + tf.desired_batch_size,
        field_pos_input: field_pos_start,
        field_pos_output: field_pos_start,
        multimatch: re.multimatch,
        non_mandatory: re.non_mandatory,
        next_start: re.next_start,
        inserters: output_field_inserters,
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
            action_buffer: &mut sess.match_set_mgr.match_sets[tf.match_set_id]
                .action_buffer,
            field_ref_offset: range
                .field_id_offset
                .map(|o| nonmax_u16_wrapping_add(o, NonMaxU16::ONE))
                .unwrap_or(NonMaxU16::ZERO),
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
                        let data = match &sv.data {
                            StreamValueData::Dropped => unreachable!(),
                            StreamValueData::Error(e) => {
                                for cgi in re
                                    .capture_group_fields
                                    .iter()
                                    .filter_map(|v| *v)
                                {
                                    sess.field_mgr.fields[cgi]
                                        .borrow_mut()
                                        .iter_hall
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
                                &b[offsets.unwrap_or(0..b.len())]
                            }
                        };
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
                        // TODO: if multimatch is false and we are in optional
                        // mode we can theoretically
                        // continue here, because there will always be exactly
                        // one match we would need
                        // StreamValueData to support null for that though
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
            | TypedSlice::Undefined(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::Object(_) => {
                rbs.field_pos_input += range.base.field_count;
                rbs.field_pos_output += range.base.field_count;
                for inserter in
                    rbs.inserters.iter_mut().filter_map(|i| i.as_mut())
                {
                    inserter.push_error(
                        OperatorApplicationError::new(
                            "regex type error",
                            op_id,
                        ),
                        range.base.field_count,
                    );
                }
            }
        }
    }
    re.next_start = rbs.next_start;
    let field_pos_input = rbs.field_pos_input;
    let produced_records = rbs.field_pos_output - field_pos_start;
    sess.match_set_mgr.match_sets[tf.match_set_id]
        .action_buffer
        .end_action_group();
    let mut base_iter = iter.into_base_iter();
    if bse || hit_stream_val {
        let unclaimed_batch_size =
            batch_size - (rbs.field_pos_input - field_pos_start);
        sess.tf_mgr.unclaim_batch_size(tf_id, unclaimed_batch_size);
        drop(rbs);
        if !hit_stream_val {
            sess.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
    } else {
        drop(rbs);
        if input_done {
            drop(input_field);
            drop(output_fields);
            sess.unlink_transform(tf_id, produced_records);
            return;
        }
        sess.tf_mgr.update_ready_state(tf_id);
    }
    if bse {
        // apply the action list first so we can move the iterator to the
        // correct continuation field
        // we explicitly don't store the iterator here so it stays at the
        // start position while we apply the action list
        drop(input_field);
        let input_field = sess.field_mgr.get_cow_field_ref(
            &mut sess.match_set_mgr,
            input_field_id,
            false,
        );
        let mut iter = sess.field_mgr.lookup_iter(
            input_field_id,
            &input_field,
            re.input_field_iter_id,
        );
        let records = iter.next_n_fields(produced_records);
        debug_assert!(records == produced_records);
        sess.field_mgr.store_iter(
            input_field_id,
            re.input_field_iter_id,
            iter,
        );
    } else {
        base_iter.move_to_field_pos(field_pos_input);
        sess.field_mgr.store_iter(
            input_field_id,
            re.input_field_iter_id,
            base_iter,
        );
    }
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
    sess.tf_mgr.push_tf_in_ready_stack(tf_id);
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
