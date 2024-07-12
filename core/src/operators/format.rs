use arrayvec::{ArrayString, ArrayVec};
use bstr::ByteSlice;
use metamatch::metamatch;
use std::{borrow::Cow, cell::RefMut, ptr::NonNull};
use unicode_ident::is_xid_start;

use smallstr::SmallString;

use super::{
    errors::{OperatorApplicationError, OperatorCreationError},
    operator::{
        OffsetInChain, OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
};
use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    job::JobData,
    liveness_analysis::{AccessFlags, LivenessData},
    options::{
        chain_settings::{SettingPrintRationalsRaw, SettingStreamBufferSize},
        session_setup::SessionSetupData,
    },
    record_data::{
        field::{Field, FieldIterRef, FieldManager},
        field_data::{
            field_value_flags, FieldValueRepr, RunLength, INLINE_STR_MAX_LEN,
        },
        field_value::{Null, Undefined},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        formattable::{
            calc_fmt_layout, FormatFillAlignment, FormatFillSpec,
            FormatOptions, Formattable, FormattingContext, NumberFormat,
            PrettyPrintFormat, RealizedFormatKey, TypeReprFormat,
            ValueFormattingOpts,
        },
        iter_hall::IterKind,
        iters::FieldIterator,
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
        },
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataType, StreamValueId, StreamValueManager,
            StreamValueUpdate,
        },
    },
    scr_error::ScrError,
    utils::{
        debuggable_nonmax::DebuggableNonMaxUsize,
        divide_by_char_len,
        int_string_conversions::{usize_to_str, USIZE_MAX_DECIMAL_DIGITS},
        io::PointerWriter,
        lazy_lock_guard::LazyRwLockGuard,
        string_store::{StringStore, StringStoreEntry},
        text_write::TextWriteIoAdapter,
        universe::CountedUniverse,
        MAX_UTF8_CHAR_LEN,
    },
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatWidthSpec {
    Value(usize),
    Ref(FormatKeyRefId),
}

impl FormatWidthSpec {
    pub fn width(&self, target_width: usize) -> usize {
        match self {
            FormatWidthSpec::Value(v) => *v,
            FormatWidthSpec::Ref(_) => target_width,
        }
    }
}

impl Default for FormatWidthSpec {
    fn default() -> Self {
        FormatWidthSpec::Value(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FormatKey {
    pub ref_idx: FormatKeyRefId,
    pub min_char_count: Option<FormatWidthSpec>,
    pub float_precision: Option<FormatWidthSpec>,
    pub opts: FormatOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatPart {
    ByteLiteral(Vec<u8>),
    TextLiteral(String),
    Key(FormatKey),
}

type FormatKeyRefId = u32;
type FormatPartIndex = u32;

#[derive(Clone)]
pub struct OpFormat {
    parts: Vec<FormatPart>,
    refs_str: Vec<Option<String>>,
    refs_idx: Vec<Option<StringStoreEntry>>,
    contains_raw_bytes: bool,
}

struct TfFormatStreamValueHandle {
    part_idx: FormatPartIndex,
    min_char_count: usize,
    float_precision: usize,
    target_sv_id: StreamValueId,
    buffering_needed: bool,
}
type TfFormatStreamValueHandleId = DebuggableNonMaxUsize;

const FINAL_OUTPUT_INDEX_NEXT_VAL: usize = usize::MAX;

#[derive(Clone, Copy)]
struct FormatError {
    kind: FieldValueRepr,
    error_in_width: bool,
    part_idx: FormatPartIndex,
}
#[derive(Default, Clone, Copy)]
struct OutputState {
    run_len: usize,
    len: usize,
    next: usize,
    min_char_count: usize,
    float_precision: usize, // for non floats, this is 'max_char_count'
    contains_raw_bytes: bool,
    // first error wins, all hope of outputting a value is lost immediately
    contained_error: Option<FormatError>,
    incomplete_stream_value_handle: Option<TfFormatStreamValueHandleId>,
}

struct OutputTarget {
    run_len: usize,
    min_char_count: usize,
    float_precision: usize,
    target: Option<NonNull<u8>>,
    remaining_len: usize,
}

pub struct TfFormat<'a> {
    op: &'a OpFormat,
    refs: Vec<FieldIterRef>,
    output_states: Vec<OutputState>,
    output_targets: Vec<OutputTarget>,
    stream_value_handles: CountedUniverse<
        TfFormatStreamValueHandleId,
        TfFormatStreamValueHandle,
    >,
    print_rationals_raw: bool,
    contains_raw_bytes: bool,
    stream_buffer_size: usize,
}

impl OutputTarget {
    fn with_writer(&mut self, f: impl FnOnce(&mut PointerWriter)) {
        let mut pw = unsafe {
            PointerWriter::new(
                self.target.unwrap().as_ptr(),
                self.remaining_len,
            )
        };
        f(&mut pw);
        self.target = Some(NonNull::new(pw.ptr()).unwrap());
        self.remaining_len = pw.remaining_bytes();
    }
}

impl FormatWidthSpec {
    pub fn realize(&self, lookup_value: usize) -> usize {
        match self {
            FormatWidthSpec::Value(v) => *v,
            FormatWidthSpec::Ref(_) => lookup_value,
        }
    }
}

impl OutputState {
    pub fn apply_to_rfk(&self, k: &FormatKey, rfk: &mut RealizedFormatKey) {
        rfk.float_precision = k.realize_float_precision(self.float_precision);
        rfk.min_char_count = k.realize_min_char_count(self.min_char_count);
    }
}

impl FormatKey {
    pub fn realize(
        &self,
        min_chars_lookup: usize,
        float_precision_lookup: usize,
    ) -> RealizedFormatKey {
        RealizedFormatKey {
            min_char_count: self.realize_min_char_count(min_chars_lookup),
            float_precision: self
                .realize_float_precision(float_precision_lookup),
            opts: self.opts.clone(),
        }
    }
    fn realize_min_char_count(&self, min_chars_lookup: usize) -> usize {
        self.min_char_count
            .as_ref()
            .map_or(0, |v| v.realize(min_chars_lookup))
    }
    fn realize_float_precision(
        &self,
        float_precision_lookup: usize,
    ) -> Option<usize> {
        self.float_precision
            .as_ref()
            .map(|v| v.realize(float_precision_lookup))
    }
    fn realize_max_char_count(&self, float_precision_lookup: usize) -> usize {
        self.realize_float_precision(float_precision_lookup)
            .unwrap_or(usize::MAX)
    }
}

// SAFETY:
// while OutputTargets Pointer is not thread safe,
// we make sure that output_states and output_targets is always empty
// when handle_tf_format exits
unsafe impl<'a> Send for TfFormat<'a> {}

pub fn setup_op_format(
    op: &mut OpFormat,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    for r in std::mem::take(&mut op.refs_str) {
        op.refs_idx
            .push(r.map(|r| sess.string_store.intern_moved(r)));
    }
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
}

pub fn build_tf_format<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFormat,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let refs: Vec<_> = op
        .refs_idx
        .iter()
        .map(|name| {
            let field_id = if let Some(name) = name {
                if let Some(id) = jd.scope_mgr.lookup_field(
                    jd.match_set_mgr.match_sets[tf_state.match_set_id]
                        .active_scope,
                    *name,
                ) {
                    jd.field_mgr.setup_field_refs(&mut jd.match_set_mgr, id);
                    let mut f = jd.field_mgr.fields[id].borrow_mut();
                    f.ref_count += 1;
                    id
                } else {
                    let field_id = jd.field_mgr.add_field(
                        tf_state.match_set_id,
                        jd.field_mgr.get_first_actor(tf_state.input_field),
                    );
                    jd.scope_mgr.insert_field_name(
                        jd.match_set_mgr.match_sets[tf_state.match_set_id]
                            .active_scope,
                        *name,
                        field_id,
                    );
                    field_id
                }
            } else {
                let mut f =
                    jd.field_mgr.fields[tf_state.input_field].borrow_mut();
                // while the ref count was already bumped by the transform
                // creation cleaning up this transform is
                // simpler this way
                f.ref_count += 1;
                tf_state.input_field
            };
            FieldIterRef {
                field_id,
                iter_id: jd.field_mgr.claim_iter_non_cow(
                    field_id,
                    IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
                ),
            }
        })
        .collect();

    let print_rationals_raw =
        jd.get_setting_from_tf_state::<SettingPrintRationalsRaw>(tf_state);
    let stream_buffer_size =
        jd.get_setting_from_tf_state::<SettingStreamBufferSize>(tf_state);
    let tf = TfFormat {
        op,
        refs,
        output_states: Vec::new(),
        output_targets: Vec::new(),
        stream_value_handles: CountedUniverse::default(),
        contains_raw_bytes: op.contains_raw_bytes,
        print_rationals_raw,
        stream_buffer_size,
    };
    TransformData::Format(tf)
}

pub fn update_op_format_variable_liveness(
    sess: &SessionData,
    fmt: &OpFormat,
    ld: &mut LivenessData,
    op_id: OperatorId,
    access_flags: &mut AccessFlags,
    op_offset_after_last_write: OffsetInChain,
) {
    access_flags.may_dup_or_drop = false;
    // might be set to true again in the loop below
    access_flags.non_stringified_input_access = false;
    access_flags.input_accessed = false;
    for p in &fmt.parts {
        match p {
            FormatPart::ByteLiteral(_) | FormatPart::TextLiteral(_) => (),
            FormatPart::Key(fk) => {
                let non_stringified = fk.min_char_count.is_some()
                    || fk.opts.add_plus_sign
                    || fk.opts.number_format != NumberFormat::Default
                    || fk.opts.zero_pad_numbers
                    || fk.opts.type_repr != TypeReprFormat::Regular;
                if let Some(name) = fmt.refs_idx[fk.ref_idx as usize] {
                    ld.access_var(
                        sess,
                        op_id,
                        ld.var_names[&name],
                        op_offset_after_last_write,
                        non_stringified,
                    );
                } else {
                    access_flags.input_accessed = true;
                    access_flags.non_stringified_input_access =
                        non_stringified;
                }
                if let Some(FormatWidthSpec::Ref(ws_ref)) = fk.min_char_count {
                    if let Some(name) = fmt.refs_idx[ws_ref as usize] {
                        ld.access_var(
                            sess,
                            op_id,
                            ld.var_names[&name],
                            op_offset_after_last_write,
                            true,
                        );
                    } else {
                        access_flags.input_accessed = true;
                        access_flags.non_stringified_input_access = true;
                    }
                }
            }
        }
    }
}

fn create_format_literal(fmt: Vec<u8>) -> FormatPart {
    match String::from_utf8(fmt) {
        Ok(v) => FormatPart::TextLiteral(v),
        Err(err) => FormatPart::ByteLiteral(err.into_bytes()),
    }
}

const NO_CLOSING_BRACE_ERR: Cow<'static, str> =
    Cow::Borrowed("format key has no closing '}'");

pub fn parse_format_width_spec<const FOR_FLOAT_PREC: bool>(
    fmt: &[u8],
    start: usize,
    refs: &mut Vec<Option<String>>,
) -> Result<(Option<FormatWidthSpec>, usize), (usize, Cow<'static, str>)> {
    let context = if FOR_FLOAT_PREC {
        "format key float precision "
    } else {
        "format key padding width"
    };
    let no_closing_dollar = |i| {
        (
            i,
            format!(
                "the identifier for the {context} has no closing '$' sign"
            )
            .into(),
        )
    };
    let mut i = start;
    let c0 = *fmt.get(i).ok_or((i, NO_CLOSING_BRACE_ERR))? as char;
    if c0.is_ascii_digit() {
        loop {
            i += 1;
            let c = *fmt.get(i).ok_or((i, NO_CLOSING_BRACE_ERR))? as char;
            if c.is_ascii_digit() {
                continue;
            }
            let val = unsafe { std::str::from_utf8_unchecked(&fmt[start..i]) };
            if c == '$' {
                let ref_id = refs.len() as FormatKeyRefId;
                refs.push(Some(val.to_owned()));
                return Ok((Some(FormatWidthSpec::Ref(ref_id)), i + 1));
            }
            let number = val.parse::<usize>().map_err(|e| {
                (
                    start,
                    format!(
                        "failed to parse the {context} as an integer: {e}"
                    )
                    .into(),
                )
            })?;
            return Ok((Some(FormatWidthSpec::Value(number)), i));
        }
    }
    let mut format_width_ident = SmallString::<[u8; 64]>::new();
    loop {
        if let Some(mut end) = fmt[i..].find_byteset("${}") {
            end += i;
            format_width_ident.push_str(fmt[i..end].to_str().map_err(
                |e| {
                    (
                        i + e.valid_up_to(),
                        format!(
                        "the identifier for the {context} must be valid utf-8"
                    )
                        .into(),
                    )
                },
            )?);
            i = end;
            let c0 = fmt[i] as char;
            if c0 == '$' {
                let fmt_ref = if format_width_ident.is_empty() {
                    None
                } else {
                    Some(format_width_ident.into_string())
                };
                let ref_id = refs.len() as FormatKeyRefId;
                refs.push(fmt_ref);
                return Ok((Some(FormatWidthSpec::Ref(ref_id)), i + 1));
            }
            let c1 =
                *fmt.get(i + 1).ok_or_else(|| no_closing_dollar(i))? as char;
            if c0 != c1 {
                return Err((
                    i,
                    format!("unmatched '{c0}' inside the identifier for the {context}, consider using '{c0}{c0}'")
                        .into(),
                ));
            }
            format_width_ident.push(c0);
            i = end + 2;
        } else {
            return Err(no_closing_dollar(i));
        }
    }
}

pub fn parse_format_flags(
    fmt: &[u8],
    start: usize,
    key: &mut FormatKey,
    refs: &mut Vec<Option<String>>,
) -> Result<usize, (usize, Cow<'static, str>)> {
    fn next(
        fmt: &[u8],
        i: &mut usize,
    ) -> Result<char, (usize, Cow<'static, str>)> {
        if fmt.len() == *i {
            return Err((*i, NO_CLOSING_BRACE_ERR));
        }
        let c = (fmt[*i..]).chars().next().unwrap();
        *i += c.len_utf8();
        Ok(c)
    }

    let mut i = start;
    let mut c = next(fmt, &mut i)?;
    if c == '}' {
        return Ok(i);
    }
    const ALIGNMENT_CHARS: [char; 3] = ['<', '^', '>'];
    const ALIGNMENT_VALS: [FormatFillAlignment; 3] = [
        FormatFillAlignment::Left,
        FormatFillAlignment::Center,
        FormatFillAlignment::Right,
    ];

    let mut align_spec = None;
    let mut fill_char = None;
    if let Some(idx) = ALIGNMENT_CHARS.iter().position(|v| *v == c) {
        align_spec = Some(ALIGNMENT_VALS[idx]);
        c = next(fmt, &mut i)?;
    } else {
        if c == '}' {
            if i + 1 == fmt.len() || fmt[i + 1] != b'}' {
                return Ok(i);
            }
            i += 1;
        }
        let c2 = next(fmt, &mut i)?;

        if let Some(idx) = ALIGNMENT_CHARS.iter().position(|v| *v == c2) {
            fill_char = Some(c);
            align_spec = Some(ALIGNMENT_VALS[idx]);
            c = next(fmt, &mut i)?;
        } else if c == '}' {
            return Err((
                i - c2.len_utf8() - 2,
                "unexpected escaped brace in format key".into(),
            ));
        } else {
            i -= c2.len_utf8();
        }
    }
    key.opts.fill = align_spec.map(|a| FormatFillSpec {
        fill_char,
        alignment: a,
    });
    if c == '+' {
        key.opts.add_plus_sign = true;
        c = next(fmt, &mut i)?;
    } else if c == '-' {
        return Err((
            i,
            "the minus sign currently has unspecified meaning in format keys"
                .into(),
        ));
    }
    if c == '#' {
        c = next(fmt, &mut i)?;
        if c == '#' {
            key.opts.pretty_print = PrettyPrintFormat::Compact;
            c = next(fmt, &mut i)?;
        } else {
            key.opts.pretty_print = PrettyPrintFormat::Pretty;
        }
    }
    if c == '0' {
        key.opts.zero_pad_numbers = true;
        c = next(fmt, &mut i)?;
    }
    if c.is_ascii_digit() || is_xid_start(c) || c == '@' {
        (key.min_char_count, i) =
            parse_format_width_spec::<false>(fmt, i - c.len_utf8(), refs)?;
        c = next(fmt, &mut i)?;
    }
    if c == '.' {
        (key.float_precision, i) =
            parse_format_width_spec::<true>(fmt, i, refs)?;
        c = next(fmt, &mut i)?;
    }
    if c == '?' {
        c = next(fmt, &mut i)?;
        if c == '?' {
            key.opts.type_repr = TypeReprFormat::Debug;
            c = next(fmt, &mut i)?;
        } else {
            key.opts.type_repr = TypeReprFormat::Typed;
        }
    } else if c != '_' && c != '}' {
        return Err((
            i - c.len_utf8(),
            format!("expected '?' or '_' before type identifier, found '{c}'")
                .into(),
        ));
    }
    if c == '%' {
        // regular repr mode, no action needed
        c = next(fmt, &mut i)?;
    }

    if c == '}' {
        return Ok(i);
    }
    let c2 = next(fmt, &mut i)?;

    let (len, num_format) = match (c, c2) {
        ('b', _) => (1, NumberFormat::Binary),
        ('0', 'b') => (2, NumberFormat::BinaryZeroB),
        ('o', _) => (1, NumberFormat::Octal),
        ('0', 'o') => (2, NumberFormat::OctalZeroO),
        ('x', _) => (1, NumberFormat::LowerHex),
        ('0', 'x') => (2, NumberFormat::LowerHexZeroX),
        ('X', _) => (1, NumberFormat::UpperHex),
        ('0', 'X') => (2, NumberFormat::UpperHexZeroX),
        ('e', _) => (1, NumberFormat::LowerExp),
        ('E', _) => (1, NumberFormat::UpperExp),
        _ => {
            return Err((
                i,
                format!("unknown number format specifier '{c}' ").into(),
            ))
        }
    };
    key.opts.number_format = num_format;
    if len == 1 {
        c = c2;
    } else {
        c = next(fmt, &mut i)?;
    }

    if c != '}' {
        return Err((
            i - c.len_utf8(),
            format!("unexpected character in format key: '{c}' ").into(),
        ));
    }
    Ok(i)
}
pub fn parse_format_key(
    fmt: &[u8],
    start: usize,
    refs: &mut Vec<Option<String>>,
) -> Result<(FormatKey, usize), (usize, Cow<'static, str>)> {
    debug_assert!(fmt[start] == b'{');
    let mut i = start + 1;
    let mut key = FormatKey::default();
    if let Some(mut end) = &fmt[i..].find_byteset("}:") {
        end += i;
        let c0 = fmt[end] as char;
        let ref_val = if end > i {
            Some(
                fmt[i..end]
                    .to_str()
                    .map_err(|e| {
                        (
                            i + e.valid_up_to(),
                            "the identifier for the format key identifier must be valid utf-8"
                                .into(),
                        )
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        refs.push(ref_val);
        key.ref_idx = refs.len() as FormatKeyRefId - 1;
        i = end + 1;
        if c0 == ':' {
            i = parse_format_flags(fmt, i, &mut key, refs)?;
        }
        return Ok((key, i));
    }
    Err((fmt.len(), NO_CLOSING_BRACE_ERR))
}

pub fn parse_format_string(
    fmt: &[u8],
    refs: &mut Vec<Option<String>>,
    parts: &mut Vec<FormatPart>,
) -> Result<(), (usize, Cow<'static, str>)> {
    let mut pending_literal = Vec::default();
    let mut i = 0;
    loop {
        let non_braced_begin = fmt[i..].find_byteset("{}");
        if let Some(mut nbb) = non_braced_begin {
            nbb += i;
            if fmt[nbb] == b'}' {
                if fmt[nbb + 1] != b'}' {
                    return Err((
                        nbb,
                        "unmatched '}', consider using '}}'".into(),
                    ));
                }
                pending_literal.extend_from_slice(&fmt[i..=nbb]);
                i = nbb + 2;
                continue;
            }
            if fmt[nbb + 1] == b'{' {
                pending_literal.extend_from_slice(&fmt[i..=nbb]);
                i = nbb + 2;
                continue;
            }
            pending_literal.extend_from_slice(&fmt[i..nbb]);
            i = nbb;
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
                pending_literal = Vec::new();
            }
            let (key, end) = parse_format_key(fmt, i, refs)?;
            parts.push(FormatPart::Key(key));
            i = end;
        } else {
            pending_literal.extend_from_slice(&fmt[i..]);
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
            }
            return Ok(());
        }
    }
}

pub fn build_op_format(
    fmt: &[u8],
    span: Span,
) -> Result<OperatorData, OperatorCreationError> {
    let mut refs_str = Vec::new();
    let mut parts = Vec::new();
    parse_format_string(fmt, &mut refs_str, &mut parts).map_err(
        |(i, msg)| OperatorCreationError {
            message: format!("format string index {i}: {msg}").into(),
            span,
        },
    )?;
    let refs_idx = Vec::with_capacity(refs_str.len());

    let contains_raw_bytes = parts.iter().any(|p| match p {
        FormatPart::ByteLiteral(_) => true,
        FormatPart::TextLiteral(_) | FormatPart::Key(_) => false,
    });

    Ok(OperatorData::Format(OpFormat {
        parts,
        refs_str,
        refs_idx,
        contains_raw_bytes,
    }))
}

pub fn parse_op_format(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let val = expr.require_single_plaintext_arg()?;
    build_op_format(val, expr.span)
}
pub fn create_op_format(
    val: &str,
) -> Result<OperatorData, OperatorCreationError> {
    build_op_format(val.as_bytes(), Span::Generated)
}
pub fn create_op_format_b(
    val: &[u8],
) -> Result<OperatorData, OperatorCreationError> {
    build_op_format(val, Span::Generated)
}

fn iter_output_states(
    fmt: &mut TfFormat,
    output_idx: &mut usize,
    run_len: RunLength,
    func: impl FnMut(&mut OutputState),
) {
    iter_output_states_advanced(
        &mut fmt.output_states,
        output_idx,
        run_len as usize,
        func,
    );
}
fn iter_output_states_advanced(
    output_states: &mut Vec<OutputState>,
    output_idx: &mut usize,
    mut run_len: usize,
    mut func: impl FnMut(&mut OutputState),
) {
    while run_len > 0 {
        let mut o = &mut output_states[*output_idx];
        if run_len < o.run_len {
            let mut o2 = *o;
            let rl_rem = o.run_len - run_len;
            o.run_len = run_len;
            o2.run_len = rl_rem;
            let o2_idx = output_states.len();
            output_states.push(o2);
            o = &mut output_states[*output_idx];
            o.next = o2_idx;
        }
        if o.contained_error.is_none()
            && o.incomplete_stream_value_handle.is_none()
        {
            func(o);
        }
        *output_idx = o.next;
        run_len -= o.run_len;
    }
}
fn iter_output_targets(
    fmt: &mut TfFormat,
    output_idx: &mut usize,
    mut run_len: usize,
    mut func: impl FnMut(&mut OutputTarget),
) {
    while run_len > 0 {
        let o = &mut fmt.output_targets[*output_idx];
        if o.target.is_some() {
            func(o)
        }
        run_len -= o.run_len;
        *output_idx += 1;
    }
}

// returns the number of *bytes* that the text will use up in the output
// including padding / truncation
fn calc_fmt_len<'a, 'b, F: Formattable<'a, 'b> + ?Sized>(
    k: &FormatKey,
    ctx: &mut F::Context,
    min_chars: usize,
    max_chars: usize,
    formatable: &F,
) -> usize {
    calc_fmt_layout(ctx, min_chars, max_chars, formatable).total_len(&k.opts)
}

fn calc_fmt_len_ost<'a, 'b, F: Formattable<'a, 'b> + ?Sized>(
    k: &FormatKey,
    ctx: &mut F::Context,
    output: &OutputState,
    formatable: &F,
) -> usize {
    calc_fmt_len(
        k,
        ctx,
        k.realize_min_char_count(output.min_char_count),
        k.realize_max_char_count(output.float_precision),
        formatable,
    )
}

pub fn lookup_width_spec(
    fm: &FieldManager,
    msm: &mut MatchSetManager,
    fmt: &mut TfFormat,
    k: &FormatKey,
    part_idx: FormatPartIndex,
    batch_size: usize,
    update_iter: bool,
    // fmt, output idx, width, run length
    succ_func: impl Fn(&mut TfFormat, &mut usize, usize, usize),
    // fmt, output idx, found field type, run length
    err_func: impl Fn(&mut TfFormat, &mut usize, FieldValueRepr, usize),
) {
    let ident_ref = if let Some(FormatWidthSpec::Ref(ident)) = k.min_char_count
    {
        fmt.refs[ident as usize]
    } else {
        return;
    };
    let field = fm.get_cow_field_ref(msm, ident_ref.field_id);
    let iter = fm
        .lookup_iter(ident_ref.field_id, &field, ident_ref.iter_id)
        .bounded(0, batch_size);
    let mut iter = AutoDerefIter::new(fm, ident_ref.field_id, iter);
    let mut output_index = 0;
    let mut handled_fields = 0;
    while let Some(range) = iter.typed_range_fwd(msm, usize::MAX, 0) {
        match range.base.data {
            FieldValueSlice::Int(ints) => {
                for (v, rl) in FieldValueRangeIter::from_range(&range, ints) {
                    let width = usize::try_from(*v).unwrap_or(0);
                    succ_func(fmt, &mut output_index, width, rl as usize);
                }
            }
            _ => err_func(
                fmt,
                &mut output_index,
                range.base.data.repr(),
                range.base.field_count,
            ),
        }
        handled_fields += range.base.field_count;
    }
    iter_output_states_advanced(
        &mut fmt.output_states,
        &mut output_index,
        batch_size - handled_fields,
        |os| {
            os.contained_error = Some(FormatError {
                error_in_width: true,
                part_idx,
                kind: FieldValueRepr::Undefined,
            })
        },
    );
    if update_iter {
        fm.store_iter(ident_ref.field_id, ident_ref.iter_id, iter);
    }
}
pub fn setup_key_output_state(
    sess: &SessionData,
    sv_mgr: &mut StreamValueManager,
    fm: &FieldManager,
    msm: &mut MatchSetManager,
    tf_id: TransformId,
    fmt: &mut TfFormat,
    batch_size: usize,
    k: &FormatKey,
    part_idx: FormatPartIndex,
) {
    lookup_width_spec(
        fm,
        msm,
        fmt,
        k,
        part_idx,
        batch_size,
        true,
        |fmt, output_idx, width, run_len| {
            iter_output_states_advanced(
                &mut fmt.output_states,
                output_idx,
                run_len,
                |os| os.min_char_count = width,
            )
        },
        |fmt, output_idx, kind, run_len| {
            iter_output_states_advanced(
                &mut fmt.output_states,
                output_idx,
                run_len,
                |os| {
                    os.contained_error = Some(FormatError {
                        error_in_width: true,
                        part_idx,
                        kind,
                    })
                },
            )
        },
    );
    let ident_ref = fmt.refs[k.ref_idx as usize];

    let field = fm.get_cow_field_ref(msm, ident_ref.field_id);
    let mut iter = AutoDerefIter::new(
        fm,
        ident_ref.field_id,
        fm.lookup_iter(ident_ref.field_id, &field, ident_ref.iter_id)
            .bounded(0, batch_size),
    );

    let mut output_index = 0;
    let mut handled_fields = 0;
    let typed_format = [TypeReprFormat::Typed, TypeReprFormat::Debug]
        .contains(&k.opts.type_repr);

    let mut string_store = LazyRwLockGuard::new(&sess.string_store);
    let mut formatting_opts = ValueFormattingOpts {
        is_stream_value: false,
        type_repr_format: k.opts.type_repr,
    };

    let mut fc = FormattingContext {
        ss: &mut string_store,
        fm,
        msm,
        print_rationals_raw: fmt.print_rationals_raw,
        is_stream_value: false,
        rfk: RealizedFormatKey {
            min_char_count: 0,
            float_precision: None,
            opts: k.opts.clone(),
        },
    };

    while let Some(range) =
        iter.typed_range_fwd(msm, usize::MAX, field_value_flags::DEFAULT)
    {
        metamatch!(match range.base.data {
            #[expand(REP in [Null, Undefined])]
            FieldValueSlice::REP(_) if typed_format => {
                iter_output_states_advanced(
                    &mut fmt.output_states,
                    &mut output_index,
                    range.base.field_count,
                    |o| {
                        o.len += calc_fmt_len_ost(k, &mut (), o, &REP);
                    },
                );
            }

            FieldValueSlice::Error(v) if typed_format => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, v)
                {
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.apply_to_rfk(k, &mut fc.rfk);
                        o.len +=
                            calc_fmt_len_ost(k, &mut formatting_opts, o, v);
                    });
                }
            }

            #[expand((REP, ITER, RAW_BYTES) in [
                (TextInline, RefAwareInlineTextIter, false),
                (TextBuffer, RefAwareTextBufferIter, false),
                (BytesInline, RefAwareInlineBytesIter, false),
                (BytesBuffer, RefAwareBytesBufferIter, false),
            ])]
            FieldValueSlice::REP(v) => {
                for (v, rl, _offs) in ITER::from_range(&range, v) {
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.apply_to_rfk(k, &mut fc.rfk);
                        o.len +=
                            calc_fmt_len_ost(k, &mut formatting_opts, o, v);
                        o.contains_raw_bytes |= RAW_BYTES;
                    });
                }
            }

            #[expand((REP, CTX) in [
                (Int, &mut fc.rfk),
                (Float, &mut fc.rfk),
                (BigInt, &mut fc.rfk),
                (BigRational, &mut fc),
                (Object, &mut fc),
                (Array, &mut fc),
                (Argument, &mut fc)
            ])]
            FieldValueSlice::REP(ints) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, ints)
                {
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.apply_to_rfk(k, &mut fc.rfk);
                        o.len += calc_fmt_len_ost(k, CTX, o, v);
                    });
                }
            }

            FieldValueSlice::StreamValueId(svs) => {
                let mut formatting_opts = ValueFormattingOpts {
                    is_stream_value: true,
                    type_repr_format: k.opts.type_repr,
                };
                for (&sv_id, rl) in
                    FieldValueRangeIter::from_range(&range, svs)
                {
                    let sv = &mut sv_mgr.stream_values[sv_id];
                    if let Some(e) = &sv.error {
                        iter_output_states(fmt, &mut output_index, rl, |o| {
                            if typed_format {
                                o.len += calc_fmt_len_ost(
                                    k,
                                    &mut formatting_opts,
                                    o,
                                    &**e,
                                );
                            } else {
                                o.contained_error = Some(FormatError {
                                    error_in_width: false,
                                    part_idx,
                                    kind: FieldValueRepr::Error,
                                });
                            }
                        });
                        continue;
                    }

                    if sv.done {
                        iter_output_states(fmt, &mut output_index, rl, |o| {
                            o.apply_to_rfk(k, &mut fc.rfk);
                            o.len += calc_fmt_len_ost(
                                k,
                                &mut fc.value_formatting_opts(),
                                o,
                                sv,
                            );
                        });
                        continue;
                    }

                    let mut need_buffering = false;

                    let mut idx = output_index;
                    iter_output_states(fmt, &mut idx, rl, |o| {
                        need_buffering |= k
                            .realize(o.min_char_count, o.float_precision)
                            .must_buffer_stream(sv);
                    });
                    // TODO: maybe figure this out?;
                    let data_type = Some(StreamValueDataType::MaybeText);
                    let len_present = if need_buffering {
                        0
                    } else {
                        sv.length_total(&mut fc.value_formatting_opts())
                    };
                    if need_buffering {
                        sv.make_buffered();
                    }
                    iter_output_states_advanced(
                        &mut fmt.output_states,
                        &mut output_index,
                        rl as usize,
                        |o| {
                            sv_mgr.subscribe_to_stream_value(
                                sv_id,
                                tf_id,
                                fmt.stream_value_handles
                                    .peek_claim_id()
                                    .into(),
                                need_buffering,
                                !need_buffering,
                            );

                            let target_sv_id = sv_mgr.claim_stream_value(
                                StreamValue::new_empty(
                                    data_type,
                                    StreamValueBufferMode::Stream,
                                ),
                            );
                            // TODO: this might be incorrect depending on
                            // padding?
                            o.len += len_present;
                            o.incomplete_stream_value_handle = Some(
                                fmt.stream_value_handles.claim_with_value(
                                    TfFormatStreamValueHandle {
                                        part_idx,
                                        target_sv_id,
                                        min_char_count: o.min_char_count,
                                        float_precision: o.float_precision,
                                        buffering_needed: need_buffering,
                                    },
                                ),
                            );
                        },
                    );
                    continue;
                }
            }

            FieldValueSlice::Custom(custom_data) => {
                for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                    &range,
                    custom_data,
                ) {
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        if let Ok(len) = v.formatted_len_raw(
                            &k.realize(o.min_char_count, o.float_precision),
                        ) {
                            o.len += len;
                        } else {
                            o.contained_error = Some(FormatError {
                                error_in_width: false,
                                part_idx,
                                kind: FieldValueRepr::Custom,
                            })
                        }
                    });
                }
            }
            FieldValueSlice::Error(_)
            | FieldValueSlice::Undefined(_)
            | FieldValueSlice::Null(_) => {
                debug_assert!(!typed_format);
                iter_output_states_advanced(
                    &mut fmt.output_states,
                    &mut output_index,
                    range.base.field_count,
                    |o| {
                        o.contained_error = Some(FormatError {
                            error_in_width: false,
                            part_idx,
                            kind: range.base.data.repr(),
                        });
                    },
                );
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        });
        handled_fields += range.base.field_count;
    }
    let uninitialized_fields = batch_size - handled_fields;
    iter_output_states_advanced(
        &mut fmt.output_states,
        &mut output_index,
        uninitialized_fields,
        |o| {
            if typed_format {
                o.len += calc_fmt_len_ost(k, &mut (), o, &Undefined);
            } else {
                o.contained_error = Some(FormatError {
                    error_in_width: false,
                    part_idx,
                    kind: FieldValueRepr::Undefined,
                })
            }
        },
    );
    // we don't store the iter state back here because we need to iterate a
    // second time for the actual write
}
fn claim_target_len(tgt: &mut OutputTarget, len: usize) {
    tgt.remaining_len = tgt
        .remaining_len
        .checked_sub(len)
        .expect("target buffer overrun");
}
fn write_bytes_to_target(tgt: &mut OutputTarget, bytes: &[u8]) {
    let len = bytes.len();
    claim_target_len(tgt, len);
    let ptr = tgt.target.unwrap().as_ptr();
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, len);
        tgt.target = Some(NonNull::new_unchecked(ptr.add(len)));
    }
}

fn write_padding_to_tgt(
    tgt: &mut OutputTarget,
    fill_char: char,
    mut len: usize,
) {
    if len == 0 {
        return;
    }
    let mut char_enc = [0u8; MAX_UTF8_CHAR_LEN];
    let char_slice = fill_char.encode_utf8(&mut char_enc);
    let mut buf = ArrayVec::<u8, 32>::new();
    let chars_cap = divide_by_char_len(buf.capacity(), char_slice.len());
    for _ in 0..chars_cap.min(len) {
        buf.extend(char_slice.as_bytes().iter().copied());
    }
    while len > chars_cap {
        write_bytes_to_target(tgt, buf.as_slice());
        len -= chars_cap;
    }
    write_bytes_to_target(tgt, &buf.as_slice()[0..len * char_slice.len()]);
}
fn setup_output_targets(
    ss: &StringStore,
    fmt: &mut TfFormat,
    sv_mgr: &mut StreamValueManager,
    op_id: OperatorId,
    output_field: &mut RefMut<Field>,
) {
    if fmt.output_states[0].run_len == 0 {
        return;
    }
    fmt.output_targets.reserve(fmt.output_states.len());
    let mut output_idx = 0;

    let starting_len =
        unsafe { output_field.iter_hall.internals_mut().data.len() };
    let mut tgt_len = starting_len;
    for os in &fmt.output_states {
        if os.contained_error.is_some() {
            tgt_len = FieldValueRepr::Error.align_size_up(tgt_len);
            tgt_len += FieldValueRepr::Error.size();
        } else if os.incomplete_stream_value_handle.is_some() {
            tgt_len = FieldValueRepr::StreamValueId.align_size_up(tgt_len);
            tgt_len += FieldValueRepr::StreamValueId.size();
        } else if os.len <= INLINE_STR_MAX_LEN {
            tgt_len += os.len;
        } else {
            tgt_len = FieldValueRepr::BytesBuffer.align_size_up(tgt_len);
            tgt_len += FieldValueRepr::BytesBuffer.size();
        }
    }
    // SAFETY:
    // in `insert_output_target` we call `push_variable_sized_type_uninit`
    // multiple times, recording the resulting pointer to fill in the data
    // later for this to work, we need to reserve all the neccessary space
    // up front
    unsafe { output_field.iter_hall.internals_mut() }
        .data
        .reserve(tgt_len - starting_len);

    loop {
        let target = unsafe {
            insert_output_target(
                fmt,
                output_idx,
                ss,
                output_field,
                op_id,
                sv_mgr,
            )
        };
        let os = &mut fmt.output_states[output_idx];
        fmt.output_targets.push(OutputTarget {
            run_len: os.run_len,
            target,
            min_char_count: os.min_char_count,
            float_precision: os.float_precision,
            remaining_len: os.len,
        });
        output_idx = os.next;
        if output_idx == FINAL_OUTPUT_INDEX_NEXT_VAL {
            break;
        }
    }
    debug_assert!(fmt.output_states.len() == fmt.output_targets.len());
}

unsafe fn insert_output_target(
    fmt: &mut TfFormat<'_>,
    output_idx: usize,
    ss: &StringStore,
    output_field: &mut RefMut<'_, Field>,
    op_id: OperatorId,
    sv_mgr: &mut StreamValueManager,
) -> Option<NonNull<u8>> {
    let os = &mut fmt.output_states[output_idx];
    if let Some(ex) = os.contained_error {
        let FormatPart::Key(k) = &fmt.op.parts[ex.part_idx as usize] else {
            unreachable!()
        };
        let mut id_str =
            ArrayString::<{ USIZE_MAX_DECIMAL_DIGITS + 1 }>::default();
        let (key_str, key_quote) =
            if let Some(part) = fmt.op.refs_idx[k.ref_idx as usize] {
                (ss.lookup(part), "'")
            } else {
                let key_index = fmt
                    .op
                    .parts
                    .iter()
                    .take(ex.part_idx as usize)
                    .filter(|p| matches!(p, FormatPart::Key(_)))
                    .count()
                    + 1;
                id_str.push('#');
                id_str.push_str(&usize_to_str(key_index));
                (id_str.as_str(), "")
            };
        let (width_ctx, width_label, width_ctx2) = if ex.error_in_width {
            if let Some(FormatWidthSpec::Ref(r)) = k.min_char_count {
                if let Some(lbl) = fmt.op.refs_idx[r as usize] {
                    (" width spec '", ss.lookup(lbl), "' of")
                } else {
                    (" width spec of", "", "")
                }
            } else {
                unreachable!()
            }
        } else {
            ("", "", "")
        };
        output_field.iter_hall.push_error(
            OperatorApplicationError::new_s(
                format!(
                    "unexpected type `{}` in{width_ctx}{width_label}{width_ctx2} format key {key_quote}{key_str}{key_quote}",
                    ex.kind
                ),
                op_id,
            ), // ENHANCE: give more context
            os.run_len,
            true,
            false,
        );
        return None;
    }
    if let Some(handle_id) = os.incomplete_stream_value_handle {
        let handle = &mut fmt.stream_value_handles[handle_id];
        let sv = &mut sv_mgr.stream_values[handle.target_sv_id];
        let mut data_inserter = sv.data_inserter(
            handle.target_sv_id,
            fmt.stream_buffer_size,
            false,
        );
        // we initialize with a placeholder ('#') value to be rust compliant
        // for now
        // PERF: maybe rethink this
        let target_ptr = if os.contains_raw_bytes {
            data_inserter.with_bytes_buffer(|buf| {
                buf.reserve(os.len);
                buf.extend(std::iter::repeat(b'#').take(os.len));
                unsafe { buf.as_mut_ptr().add(buf.len() - os.len) }
            })
        } else {
            data_inserter
                .with_text_buffer(|buf| {
                    buf.reserve(os.len);

                    buf.extend(std::iter::repeat('#').take(os.len));
                    unsafe { buf.as_mut_ptr().add(buf.len() - os.len) }
                })
                .unwrap()
        };

        let target = unsafe { Some(NonNull::new_unchecked(target_ptr)) };
        // just to be 'rust compliant' ...

        output_field.iter_hall.push_stream_value_id(
            handle.target_sv_id,
            os.run_len,
            true,
            false,
        );
        return target;
    }
    if os.len <= INLINE_STR_MAX_LEN {
        let target = unsafe {
            Some(NonNull::new_unchecked(
                // SAFETY: when this function is called multiple times,
                // the caller needs to make sure that enough space is
                // reserved up front. otherwise, an internal realloc could
                // invalidate the pointer that we get here
                // see `setup_output_targets` for how we currently do this
                output_field.iter_hall.push_variable_sized_type_uninit(
                    if os.contains_raw_bytes {
                        FieldValueRepr::BytesInline
                    } else {
                        FieldValueRepr::TextInline
                    },
                    os.len,
                    os.run_len,
                    true,
                ),
            ))
        };
        return target;
    }
    if os.contains_raw_bytes {
        let mut buf = Vec::with_capacity(os.len);
        let target = unsafe { Some(NonNull::new_unchecked(buf.as_mut_ptr())) };
        // just to be 'rust compliant' ...
        buf.extend(std::iter::repeat(0).take(os.len));
        output_field
            .iter_hall
            .push_bytes_buffer(buf, os.run_len, true, false);
        return target;
    }
    let mut buf = String::with_capacity(os.len);
    let target = unsafe { Some(NonNull::new_unchecked(buf.as_mut_ptr())) };
    // just to be 'rust compliant' ...
    buf.extend(std::iter::repeat('#').take(os.len));
    output_field
        .iter_hall
        .push_string(buf, os.run_len, true, false);
    target
}
fn write_formatted<'a, 'b, F: Formattable<'a, 'b> + ?Sized>(
    k: &FormatKey,
    ctx: &mut F::Context,
    tgt: &mut OutputTarget,
    formatable: &F,
) {
    let fill_spec = k.opts.fill.as_ref().copied().unwrap_or_default();
    let layout = calc_fmt_layout(
        ctx,
        k.realize_min_char_count(tgt.min_char_count),
        k.realize_max_char_count(tgt.float_precision),
        formatable,
    );
    let (pad_left, pad_right) = fill_spec.distribute_padding(layout.padding);

    write_padding_to_tgt(tgt, fill_spec.get_fill_char(), pad_left);
    tgt.with_writer(|pw| formatable.format(ctx, pw).unwrap());
    write_padding_to_tgt(tgt, fill_spec.get_fill_char(), pad_right);
}

fn write_fmt_key(
    sess: &SessionData,
    sv_mgr: &StreamValueManager,
    fm: &FieldManager,
    msm: &mut MatchSetManager,
    fmt: &mut TfFormat,
    batch_size: usize,
    k: &FormatKey,
    part_idx: FormatPartIndex,
) {
    // any potential unconsumed input was already set during width calculation
    lookup_width_spec(
        fm,
        msm,
        fmt,
        k,
        part_idx,
        batch_size,
        false,
        |fmt, output_idx, width, run_len| {
            iter_output_targets(fmt, output_idx, run_len, |ot| {
                ot.min_char_count = width
            })
        },
        |_fmt, _output_idx, _kind, _run_len| (),
    );
    let ident_ref = fmt.refs[k.ref_idx as usize];

    let field = fm.get_cow_field_ref(msm, ident_ref.field_id);
    let base_iter = fm
        .lookup_iter(ident_ref.field_id, &field, ident_ref.iter_id)
        .bounded(0, batch_size);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut iter = AutoDerefIter::new(fm, ident_ref.field_id, base_iter);
    let type_repr = [TypeReprFormat::Typed, TypeReprFormat::Debug]
        .contains(&k.opts.type_repr);
    let mut output_index = 0;
    let mut string_store = LazyRwLockGuard::new(&sess.string_store);
    let mut formatting_opts = ValueFormattingOpts {
        is_stream_value: false,
        type_repr_format: k.opts.type_repr,
    };
    while let Some(range) =
        iter.typed_range_fwd(msm, usize::MAX, field_value_flags::DEFAULT)
    {
        metamatch!(match range.base.data {
            #[expand(REP in [Null, Undefined])]
            FieldValueSlice::REP(_) => {
                iter_output_targets(
                    fmt,
                    &mut output_index,
                    range.base.field_count,
                    |tgt| {
                        write_formatted(k, &mut (), tgt, &REP);
                    },
                );
            }

            #[expand((REP, ITER) in [
                (TextInline, RefAwareInlineTextIter),
                (BytesInline, RefAwareInlineBytesIter),
                (TextBuffer, RefAwareTextBufferIter),
                (BytesBuffer, RefAwareBytesBufferIter),
            ])]
            FieldValueSlice::REP(text) => {
                for (v, rl, _offs) in ITER::from_range(&range, text) {
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| {
                            write_formatted(k, &mut formatting_opts, tgt, v);
                        },
                    );
                }
            }

            #[expand(REP in [Int, Float, BigInt])]
            FieldValueSlice::REP(ints) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, ints)
                {
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| {
                            let mut rfk = k.realize(
                                tgt.min_char_count,
                                tgt.float_precision,
                            );
                            write_formatted(k, &mut rfk, tgt, v)
                        },
                    );
                }
            }

            #[expand(REP in [BigRational, Object, Array, Argument])]
            FieldValueSlice::REP(vs) => {
                let mut fc = FormattingContext {
                    ss: &mut string_store,
                    fm,
                    msm,
                    print_rationals_raw: fmt.print_rationals_raw,
                    is_stream_value: false,
                    rfk: RealizedFormatKey::default(),
                };
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, vs)
                {
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| {
                            write_formatted(k, &mut fc, tgt, v);
                        },
                    );
                }
            }

            FieldValueSlice::Error(errs) => {
                for (v, rl) in
                    RefAwareFieldValueRangeIter::from_range(&range, errs)
                {
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| write_formatted(k, &mut formatting_opts, tgt, v),
                    );
                }
            }

            FieldValueSlice::Custom(custom_data) => {
                let mut rfk = RealizedFormatKey {
                    opts: k.opts.clone(),
                    min_char_count: 0,
                    float_precision: None,
                };
                for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                    &range,
                    custom_data,
                ) {
                    let len = v.formatted_len_raw(&rfk).expect("second invocation of formatted_len_raw failed while first one succeeded");
                    let mut prev_target: Option<*mut u8> = None;
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| unsafe {
                            rfk.min_char_count =
                                k.realize_min_char_count(tgt.min_char_count);
                            rfk.float_precision =
                                k.realize_float_precision(tgt.float_precision);
                            claim_target_len(tgt, len);
                            let ptr = tgt.target.unwrap().as_ptr();
                            if let Some(src) = prev_target {
                                std::ptr::copy_nonoverlapping(src, ptr, len);
                            } else {
                                let mut w = TextWriteIoAdapter(
                                    PointerWriter::new(ptr, len),
                                );
                                let res = v.format_raw(&mut w, &rfk);
                                res.expect("custom format failed despite sufficient space");
                                assert!(
                                    w.0.remaining_bytes() == 0,
                                    "custom format lied about len",
                                );
                                prev_target = Some(ptr);
                            }
                            tgt.target =
                                Some(NonNull::new_unchecked(ptr.add(len)));
                        },
                    );
                }
            }

            FieldValueSlice::StreamValueId(svs) => {
                let mut formatting_opts = ValueFormattingOpts {
                    is_stream_value: true,
                    type_repr_format: k.opts.type_repr,
                };
                for (sv_id, rl) in FieldValueRangeIter::from_range(&range, svs)
                {
                    let sv = &sv_mgr.stream_values[*sv_id];

                    if let Some(error) = &sv.error {
                        iter_output_targets(
                            fmt,
                            &mut output_index,
                            rl as usize,
                            |tgt| {
                                write_formatted(
                                    k,
                                    &mut formatting_opts,
                                    tgt,
                                    &**error,
                                )
                            },
                        );
                        continue;
                    }
                    iter_output_targets(
                        fmt,
                        &mut output_index,
                        rl as usize,
                        |tgt| {
                            let write_eagerly = !k
                                .realize(
                                    tgt.min_char_count,
                                    tgt.float_precision,
                                )
                                .must_buffer_stream(sv);
                            if write_eagerly {
                                write_formatted(
                                    k,
                                    &mut formatting_opts,
                                    tgt,
                                    sv,
                                );
                            }
                            if !sv.done {
                                tgt.target = None;
                            }
                        },
                    );
                }
            }
            FieldValueSlice::FieldReference(_)
            | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
        });
    }
    let base_iter = iter.into_base_iter();
    let field_pos_end = base_iter.get_next_field_pos();
    fm.store_iter(ident_ref.field_id, ident_ref.iter_id, base_iter);
    if type_repr {
        let unconsumed_fields = batch_size - (field_pos_end - field_pos_start);
        iter_output_targets(
            fmt,
            &mut output_index,
            unconsumed_fields,
            |tgt| write_formatted(k, &mut (), tgt, &Undefined),
        );
    }
}
fn drop_field_refs(jd: &mut JobData, fmt: &mut TfFormat) {
    for r in &fmt.refs {
        jd.field_mgr
            .drop_field_refcount(r.field_id, &mut jd.match_set_mgr);
    }
}

pub fn format_add_var_names(
    fmt: &crate::operators::format::OpFormat,
    ld: &mut LivenessData,
) {
    for r in &fmt.refs_idx {
        ld.add_var_name_opt(*r);
    }
}

pub fn handle_tf_format(
    jd: &mut JobData,
    tf_id: TransformId,
    fmt: &mut TfFormat,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let tf = &jd.tf_mgr.transforms[tf_id];
    let mut output_field = jd.field_mgr.fields[tf.output_field].borrow_mut();
    fmt.output_states.push(OutputState {
        run_len: batch_size,
        next: FINAL_OUTPUT_INDEX_NEXT_VAL,
        len: 0,
        min_char_count: 0,
        float_precision: 0,
        contains_raw_bytes: fmt.contains_raw_bytes,
        contained_error: None,
        incomplete_stream_value_handle: None,
    });
    for (part_idx, part) in fmt.op.parts.iter().enumerate() {
        match part {
            FormatPart::ByteLiteral(v) => {
                fmt.output_states.iter_mut().for_each(|s| {
                    if s.incomplete_stream_value_handle.is_none() {
                        s.len += v.len();
                        s.contains_raw_bytes = true;
                    }
                })
            }
            FormatPart::TextLiteral(v) => {
                fmt.output_states.iter_mut().for_each(|s| {
                    if s.incomplete_stream_value_handle.is_none() {
                        s.len += v.len()
                    }
                });
            }
            FormatPart::Key(k) => setup_key_output_state(
                jd.session_data,
                &mut jd.sv_mgr,
                &jd.field_mgr,
                &mut jd.match_set_mgr,
                tf_id,
                fmt,
                batch_size,
                k,
                part_idx as FormatPartIndex,
            ),
        }
    }
    setup_output_targets(
        &jd.session_data.string_store.read().unwrap(),
        fmt,
        &mut jd.sv_mgr,
        tf.op_id.unwrap(),
        &mut output_field,
    );
    for (part_idx, part) in fmt.op.parts.iter().enumerate() {
        match part {
            FormatPart::ByteLiteral(v) => {
                iter_output_targets(fmt, &mut 0, batch_size, |tgt| {
                    write_bytes_to_target(tgt, v);
                });
            }
            FormatPart::TextLiteral(v) => {
                iter_output_targets(fmt, &mut 0, batch_size, |tgt| {
                    write_bytes_to_target(tgt, v.as_bytes());
                });
            }
            FormatPart::Key(k) => write_fmt_key(
                jd.session_data,
                &jd.sv_mgr,
                &jd.field_mgr,
                &mut jd.match_set_mgr,
                fmt,
                batch_size,
                k,
                part_idx as FormatPartIndex,
            ),
        }
    }
    fmt.output_states.clear();
    fmt.output_targets.clear();
    drop(output_field);
    let streams_done = fmt.stream_value_handles.is_empty();
    if streams_done {
        if ps.input_done {
            drop_field_refs(jd, fmt);
        } else if ps.next_batch_ready {
            jd.tf_mgr.push_tf_in_ready_stack(tf_id);
        }
    }
    jd.tf_mgr
        .submit_batch(tf_id, batch_size, ps.input_done && streams_done);
}

pub fn handle_tf_format_stream_value_update<'a>(
    jd: &mut JobData<'a>,
    fmt: &mut TfFormat<'a>,
    update: StreamValueUpdate,
) {
    let handle_id = TfFormatStreamValueHandleId::new(update.custom).unwrap();
    let handle = &mut fmt.stream_value_handles[handle_id];
    let tf_id = update.tf_id;

    let (in_sv_id, out_sv_id) = (update.sv_id, handle.target_sv_id);
    let (sv, out_sv) = jd
        .sv_mgr
        .stream_values
        .get_two_distinct_mut(in_sv_id, out_sv_id);
    let (in_sv, out_sv) = (sv.unwrap(), out_sv.unwrap());
    let done = in_sv.done;

    if out_sv.propagate_error(&in_sv.error) {
        jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
        jd.sv_mgr
            .drop_field_value_subscription(in_sv_id, Some(update.tf_id));
        jd.sv_mgr.drop_field_value_subscription(out_sv_id, None);
        return;
    }

    let FormatPart::Key(format_key) = &fmt.op.parts[handle.part_idx as usize]
    else {
        unreachable!();
    };

    if out_sv.propagate_error(&in_sv.error) {
        jd.sv_mgr.inform_stream_value_subscribers(out_sv_id);
        jd.sv_mgr
            .drop_field_value_subscription(in_sv_id, Some(update.tf_id));
        jd.sv_mgr.drop_field_value_subscription(out_sv_id, None);
        fmt.stream_value_handles.release(handle_id);
        return;
    }

    let mut inserter =
        out_sv.data_inserter(out_sv_id, fmt.stream_buffer_size, true);

    if handle.buffering_needed {
        debug_assert!(in_sv.is_buffered() && in_sv.done);
        let mut string_store =
            LazyRwLockGuard::new(&jd.session_data.string_store);
        let fc = FormattingContext {
            ss: &mut string_store,
            fm: &jd.field_mgr,
            msm: &jd.match_set_mgr,
            print_rationals_raw: fmt.print_rationals_raw,
            is_stream_value: true,
            rfk: format_key
                .realize(handle.min_char_count, handle.float_precision),
        };
        let len = calc_fmt_len(
            format_key,
            &mut fc.value_formatting_opts(),
            format_key.realize_min_char_count(handle.min_char_count),
            format_key.realize_max_char_count(handle.float_precision),
            in_sv,
        );

        let mut output_target = OutputTarget {
            run_len: 1,
            min_char_count: handle.min_char_count,
            float_precision: handle.float_precision,
            target: None,
            remaining_len: len,
        };
        let mut vfo = fc.value_formatting_opts();
        // PERF: we should support outputting chunks
        // HACK // TODO: use a proper mechanism for this
        // that supports text aswell
        inserter.with_bytes_buffer(|buf| {
            buf.reserve(len);
            unsafe {
                output_target.target = Some(NonNull::new_unchecked(
                    buf.as_mut_ptr().add(buf.len()),
                ));
                write_formatted(
                    format_key,
                    &mut vfo,
                    &mut output_target,
                    in_sv,
                );
                buf.set_len(buf.len() + len);
            };
        });
    } else {
        let stream_type = in_sv.data_type.unwrap();
        let type_repr = format_key.opts.type_repr;
        let print_plain = type_repr == TypeReprFormat::Regular;

        let mut iter = in_sv.data_cursor_from_update(&update);

        while let Some(chunk) = iter.next_steal(inserter.may_append_buffer()) {
            match chunk {
                StreamValueData::StaticBytes(_)
                | StreamValueData::Bytes { .. } => {
                    if print_plain {
                        inserter.append(chunk);
                    } else {
                        inserter.append(chunk.as_escaped_text(b'"'));
                    }
                }
                StreamValueData::StaticText { .. }
                | StreamValueData::Text { .. } => {
                    inserter.append(chunk);
                }
            };
        }

        if done && !print_plain {
            match stream_type {
                StreamValueDataType::Text | StreamValueDataType::Bytes => {
                    inserter.append(StreamValueData::StaticText("\""))
                }
                StreamValueDataType::VariableTypeArray
                | StreamValueDataType::SingleValue(_)
                | StreamValueDataType::FixedTypeArray(_) => todo!(),
                StreamValueDataType::MaybeText => unreachable!(),
            }
        }
    }

    if !done {
        drop(inserter);
        jd.sv_mgr
            .inform_stream_value_subscribers(handle.target_sv_id);
        return;
    }

    let mut i = handle.part_idx as usize + 1;
    while i < fmt.op.parts.len() {
        match &fmt.op.parts[i] {
            FormatPart::ByteLiteral(l) => {
                inserter.append(StreamValueData::StaticBytes(l));
            }
            FormatPart::TextLiteral(l) => {
                inserter.append(StreamValueData::StaticText(l));
            }
            FormatPart::Key(_k) => {
                todo!();
            }
        }
        i += 1;
    }
    drop(inserter);

    handle.part_idx = i as FormatPartIndex;
    out_sv.done = true;
    jd.sv_mgr
        .inform_stream_value_subscribers(handle.target_sv_id);
    jd.sv_mgr
        .drop_field_value_subscription(in_sv_id, Some(tf_id));
    jd.sv_mgr
        .drop_field_value_subscription(handle.target_sv_id, None);
    fmt.stream_value_handles.release(handle_id);
    if fmt.stream_value_handles.is_empty() {
        jd.tf_mgr.push_tf_in_ready_stack(tf_id);
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use crate::operators::format::{
        FormatFillAlignment, FormatFillSpec, FormatKey, FormatOptions,
        FormatWidthSpec,
    };

    use super::{parse_format_string, FormatPart};

    #[test]
    fn empty_format_string() {
        let mut dummy = Vec::new();
        let mut parts = Vec::new();
        parse_format_string(&[], &mut dummy, &mut parts).unwrap();
        assert_eq!(&parts, &[]);
    }

    #[test]
    fn single_literal() {
        for (lit, res) in [
            ("f", "f"),
            ("foo", "foo"),
            ("{{", "{"),
            ("{{{{", "{{"),
            ("}}", "}"),
            ("}}}}", "}}"),
            ("foo{{", "foo{"),
            ("foo{{bar", "foo{bar"),
            ("{{foo{{{{bar}}baz}}}}", "{foo{{bar}baz}}"),
        ] {
            let mut dummy = Vec::new();
            let mut parts = Vec::new();
            parse_format_string(lit.as_bytes(), &mut dummy, &mut parts)
                .unwrap();
            assert_eq!(&parts, &[FormatPart::TextLiteral(res.to_owned())]);
        }
    }

    #[test]
    fn two_keys() {
        let mut idents = Vec::new();
        let a = FormatKey {
            ref_idx: 0,
            ..Default::default()
        };
        let b = FormatKey {
            ref_idx: 1,
            ..Default::default()
        };
        let mut parts = Vec::new();
        parse_format_string(
            "foo{{{a}}}__{b}".as_bytes(),
            &mut idents,
            &mut parts,
        )
        .unwrap();
        assert_eq!(
            &parts,
            &[
                FormatPart::TextLiteral("foo{".to_owned()),
                FormatPart::Key(a),
                FormatPart::TextLiteral("}__".to_owned()),
                FormatPart::Key(b),
            ]
        );
        assert_eq!(idents, &[Some("a".to_owned()), Some("b".to_owned())])
    }

    #[test]
    fn fill_char() {
        let mut idents = Vec::new();
        let a = FormatKey {
            min_char_count: Some(FormatWidthSpec::Ref(1)),
            opts: FormatOptions {
                fill: Some(FormatFillSpec::new(
                    Some('~'),
                    FormatFillAlignment::Center,
                )),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut parts = Vec::new();
        parse_format_string(
            "{foo:~^bar$}".as_bytes(),
            &mut idents,
            &mut parts,
        )
        .unwrap();
        assert_eq!(parts, &[FormatPart::Key(a)]);
    }

    #[test]
    fn fill_options() {
        let mut idents = Vec::new();
        let a = FormatKey {
            ref_idx: 0,
            min_char_count: Some(FormatWidthSpec::Value(5)),
            opts: FormatOptions {
                fill: Some(FormatFillSpec::new(
                    Some('+'),
                    FormatFillAlignment::Left,
                )),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut parts = Vec::new();
        parse_format_string("{a:+<5}".as_bytes(), &mut idents, &mut parts)
            .unwrap();
        assert_eq!(parts, &[FormatPart::Key(a),]);
        assert_eq!(idents, &[Some("a".to_owned())])
    }

    #[test]
    fn float_precision() {
        let mut idents = Vec::new();
        let a = FormatKey {
            ref_idx: 0,
            min_char_count: Some(FormatWidthSpec::Value(3)),
            float_precision: Some(FormatWidthSpec::Ref(1)),
            ..Default::default()
        };
        let mut parts = Vec::new();
        parse_format_string("{a:3.b$}".as_bytes(), &mut idents, &mut parts)
            .unwrap();
        assert_eq!(&parts, &[FormatPart::Key(a)]);
        assert_eq!(idents, &[Some("a".to_owned()), Some("b".to_owned())])
    }

    #[test]
    fn width_not_an_ident() {
        let mut idents = Vec::new();
        let mut parts = Vec::new();

        assert_eq!(
            parse_format_string("{a:1x$}".as_bytes(), &mut idents, &mut parts),
            Err((
                4,
                Cow::Borrowed(
                    "expected '?' or '_' before type identifier, found 'x'"
                )
            )) // ENHANCE: better error message for this case
        );
    }

    #[test]
    fn fill_char_is_optional_not_an_ident() {
        let mut idents = Vec::new();
        let mut parts = Vec::new();
        let a = FormatKey {
            min_char_count: Some(FormatWidthSpec::Value(2)),
            opts: FormatOptions {
                fill: Some(FormatFillSpec::new(
                    None,
                    FormatFillAlignment::Center,
                )),
                ..Default::default()
            },
            ..Default::default()
        };
        parse_format_string("{a:^2}".as_bytes(), &mut idents, &mut parts)
            .unwrap();
        assert_eq!(&parts, &[FormatPart::Key(a)]);
    }
}
