use arrayvec::ArrayVec;
use bstr::ByteSlice;
use nonmax::NonMaxUsize;
use std::{borrow::Cow, cell::RefMut, fmt::Write, ptr::NonNull};

use smallstr::SmallString;

use crate::{
    field_data::{
        field_value_flags,
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::{PushInterface, RawPushInterface},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
        FieldValueKind, RunLength, INLINE_STR_MAX_LEN,
    },
    job_session::{Field, FieldId, FieldManager, JobData, MatchSetManager, StreamValueManager},
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareStreamValueIter,
    },
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::{
        divide_by_char_len, i64_digits, i64_to_str,
        string_store::{StringStore, StringStoreEntry},
        u64_to_str,
        universe::Universe,
        LengthAndCharsCountingWriter, LengthCountingWriter, ValueProducingCallable,
        MAX_UTF8_CHAR_LEN,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, OperatorId},
    print::typed_slice_zst_str,
    transform::{TransformData, TransformId, TransformState},
    utils::{ERROR_PREFIX_STR, NULL_STR},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FormatFillAlignment {
    #[default]
    Right,
    Left,
    Center,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FormatFillSpec {
    fill_char: Option<char>,
    alignment: FormatFillAlignment,
}

impl FormatFillSpec {
    pub fn new(fill_char: Option<char>, alignment: FormatFillAlignment) -> Self {
        Self {
            fill_char: fill_char,
            alignment,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatWidthSpec {
    Value(usize),
    Ref(FormatKeyRefId),
}

impl FormatWidthSpec {
    pub fn width(&self, width_lookup: usize) -> usize {
        match self {
            FormatWidthSpec::Value(v) => *v,
            FormatWidthSpec::Ref(_) => width_lookup,
        }
    }
}

impl Default for FormatWidthSpec {
    fn default() -> Self {
        FormatWidthSpec::Value(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FormatType {
    #[default]
    Default, // the default value representation
    Debug,     // add typing information e.g. "42" insead of 42 (the string)
    MoreDebug, // like debug, but prefix >>> for stream values
    Binary,    // print integers with base 2, e.g 101010 instead of 42
    Octal,     // print integers with base 8, e.g 52 instead of 42
    Hex,       // print integers in lower case hexadecimal, e.g 2a instead of 42
    UpperHex,  // print integers in upper case hexadecimal, e.g 2A instead of 42
    LowerExp,  // print numbers in upper case scientific notation, e.g. 4.2e1 instead of 42
    UpperExp,  // print numbers in lower case scientific notation, e.g. 4.2E1 instead of 42
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FormatKey {
    identifier: FormatKeyRefId,
    fill: Option<FormatFillSpec>,
    add_plus_sign: bool,
    zero_pad_numbers: bool,
    width: Option<FormatWidthSpec>,
    float_precision: Option<FormatWidthSpec>,
    alternate_form: bool, // prefix 0x for hex, 0o for octal and 0b for binary, pretty print objects / arrays
    format_type: FormatType,
    debug: bool,
    unicode: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatPart {
    ByteLiteral(Vec<u8>),
    TextLiteral(String),
    Key(FormatKey),
}

type FormatKeyRefId = usize;

#[derive(Clone)]
pub struct OpFormat {
    pub parts: Vec<FormatPart>,
    pub refs_str: Vec<Option<String>>,
    pub refs_idx: Vec<Option<StringStoreEntry>>,
}

#[derive(Clone, Copy)]
pub struct FormatIdentRef {
    field_id: FieldId,
    iter_id: IterId,
}

struct TfFormatStreamValueHandle {
    part_idx: usize,
    handled_len: usize,
    width_lookup: usize,
    target_sv_id: StreamValueId,
    wait_to_end: bool,
}
type TfFormatStreamValueHandleId = NonMaxUsize;

const FINAL_OUTPUT_INDEX_NEXT_VAL: usize = usize::MAX;

#[derive(Clone, Copy)]
struct OutputState {
    next: usize,
    len: usize,
    width_lookup: usize,
    run_len: usize,
    contains_raw_bytes: bool,
    error_occured: bool,
    incomplete_stream_value_handle: Option<TfFormatStreamValueHandleId>,
}

struct OutputTarget {
    run_len: usize,
    width_lookup: usize,
    target: Option<NonNull<u8>>,
}

impl Default for OutputState {
    fn default() -> Self {
        Self {
            next: 0,
            len: Default::default(),
            contains_raw_bytes: false,
            error_occured: false,
            incomplete_stream_value_handle: None,
            run_len: 0,
            width_lookup: 0,
        }
    }
}

pub struct TfFormat<'a> {
    parts: &'a Vec<FormatPart>,
    refs: Vec<FormatIdentRef>,
    output_states: Vec<OutputState>,
    output_targets: Vec<OutputTarget>,
    stream_value_handles: Universe<TfFormatStreamValueHandleId, TfFormatStreamValueHandle>,
}
// SAFETY:
// while OutputTargets Pointer is not thread safe,
// we make sure that output_states and output_targets is always empty
// when handle_tf_format exits
unsafe impl<'a> Send for TfFormat<'a> {}

pub fn setup_op_format(
    string_store: &mut StringStore,
    op: &mut OpFormat,
) -> Result<(), OperatorSetupError> {
    for r in std::mem::replace(&mut op.refs_str, Default::default()).into_iter() {
        op.refs_idx.push(r.map(|r| string_store.intern_moved(r)));
    }
    Ok(())
}

pub fn setup_tf_format<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFormat,
    tf_id: TransformId,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    let refs: Vec<_> = op
        .refs_idx
        .iter()
        .map(|name| {
            let (field_id, mut f) = if let Some(name) = name {
                let (id, mut f) = if let Some(id) = sess.match_set_mgr.match_sets
                    [tf_state.match_set_id]
                    .field_name_map
                    .get(name)
                    .cloned()
                {
                    let mut f = sess.field_mgr.fields[id].borrow_mut();
                    f.ref_count += 1;
                    (id, f)
                } else {
                    let id = sess.field_mgr.add_field(
                        tf_state.match_set_id,
                        sess.field_mgr.get_min_apf_idx(tf_state.input_field),
                    );
                    sess.match_set_mgr
                        .add_field_name(&sess.field_mgr, id, *name);
                    let mut f = sess.field_mgr.fields[id].borrow_mut();
                    f.added_as_placeholder_by_tf = Some(tf_id);
                    (id, f)
                };
                (id, f)
            } else {
                let mut f = sess.field_mgr.fields[tf_state.input_field].borrow_mut();
                // while the ref count was already bumped by the transform creation
                // cleaning up this transform is simpler this way
                f.ref_count += 1;
                (tf_state.input_field, f)
            };
            FormatIdentRef {
                field_id,
                iter_id: f.field_data.claim_iter(),
            }
        })
        .collect();
    let tf = TfFormat {
        parts: &op.parts,
        refs,
        output_states: Default::default(),
        output_targets: Default::default(),
        stream_value_handles: Default::default(),
    };
    TransformData::Format(tf)
}

fn create_format_literal(fmt: Vec<u8>) -> FormatPart {
    match String::from_utf8(fmt) {
        Ok(v) => FormatPart::TextLiteral(v),
        Err(err) => FormatPart::ByteLiteral(err.into_bytes()),
    }
}

const NO_CLOSING_BRACE_ERR: Cow<'static, str> = Cow::Borrowed("format key has no closing '}'");

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
            format!("the identifier for the {context} has no closing '$' sign")
                .to_string()
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
                let ref_id = refs.len();
                refs.push(Some(val.to_owned()));
                return Ok((Some(FormatWidthSpec::Ref(ref_id)), i as usize + 1));
            }
            let number = val.parse::<usize>().map_err(|e| {
                (
                    start,
                    format!("failed to parse the {context} as an integer: {e}").into(),
                )
            })?;
            return Ok((Some(FormatWidthSpec::Value(number)), i as usize));
        }
    }
    let mut format_width_ident = SmallString::<[u8; 64]>::new();
    loop {
        if let Some(mut end) = (&fmt[i..]).find_byteset("${}") {
            end += i;
            format_width_ident.push_str((&fmt[i..end]).to_str().map_err(|e| {
                (
                    i + e.valid_up_to(),
                    format!("the identifier for the {context} must be valid utf-8").into(),
                )
            })?);
            i = end;
            let c0 = fmt[i] as char;
            if c0 == '$' {
                let fmt_ref = if format_width_ident.is_empty() {
                    None
                } else {
                    Some(format_width_ident.into_string())
                };
                let ref_id = refs.len();
                refs.push(fmt_ref);
                return Ok((Some(FormatWidthSpec::Ref(ref_id)), i + 1));
            }
            let c1 = *fmt.get(i + 1).ok_or_else(|| no_closing_dollar(i))? as char;
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
    fn next(fmt: &[u8], i: usize) -> Result<char, (usize, Cow<'static, str>)> {
        Ok(*fmt.get(i).ok_or((i, NO_CLOSING_BRACE_ERR))? as char)
    }

    debug_assert!(fmt[start] == ':' as u8);
    let mut i = start + 1;
    let mut c = next(fmt, i)?;
    if c == '}' {
        return Ok(1);
    }
    const ALIGNMENT_SPECS: [char; 3] = ['<', '>', '^'];

    let mut align_spec = None;
    let mut align_char = None;
    if ALIGNMENT_SPECS.contains(&c) {
        align_spec = Some(c);
        i += 1;
    } else if c != '}' {
        let c2 = next(fmt, i + 1)?;
        if ALIGNMENT_SPECS.contains(&c2) {
            align_char = Some(c);
            align_spec = Some(c2);
            i += 2;
        }
    }
    key.fill = match align_spec {
        Some('<') => Some(FormatFillSpec::new(align_char, FormatFillAlignment::Left)),
        Some('^') => Some(FormatFillSpec::new(align_char, FormatFillAlignment::Center)),
        Some('>') => Some(FormatFillSpec::new(align_char, FormatFillAlignment::Right)),
        _ => None,
    };
    if key.fill.is_some() {
        c = next(fmt, i)?;
        if c == '}' {
            return Ok(1);
        }
    }
    if c == '+' {
        key.add_plus_sign = true;
        i += 1;
        c = next(fmt, i)?;
    } else if c == '-' {
        return Err((
            i,
            "the minus sign currently has unspecified meaning in format keys".into(),
        ));
    }
    if c == '#' {
        key.alternate_form = true;
        i += 1;
        c = next(fmt, i)?;
    }
    if c == '0' {
        key.zero_pad_numbers = true;
        i += 1;
        c = next(fmt, i)?;
    }
    let c2 = next(fmt, i + 1)?;
    if !".}?".contains(c) && c2 != '?' {
        (key.width, i) = parse_format_width_spec::<false>(fmt, i, refs)?;
        c = next(fmt, i)?;
    }
    if c == '.' {
        (key.float_precision, i) = parse_format_width_spec::<true>(fmt, i + 1, refs)?;
        c = next(fmt, i)?;
    }

    if c == '?' && c2 != '?' {
        key.format_type = FormatType::Debug;
        i += 1;
        c = next(fmt, i)?;
    } else {
        if c == '}' {
            return Ok(i);
        }
        let c2 = next(fmt, i + 1)?;
        if c2 != '?' {
            return Err((i, format!("expected '?' after type specifier '{c}'").into()));
        }
        match c {
            '?' => key.format_type = FormatType::MoreDebug,
            'x' => key.format_type = FormatType::Hex,
            'X' => key.format_type = FormatType::UpperHex,
            'o' => key.format_type = FormatType::Octal,
            'b' => key.format_type = FormatType::Binary,
            'e' => key.format_type = FormatType::LowerExp,
            'E' => key.format_type = FormatType::UpperExp,
            _ => return Err((i, format!("unknown type specifier '{c}?' ").into())),
        }
        i += 2;
        c = next(fmt, i)?;
    }
    if c != '}' {
        return Err((
            i,
            format!("expected '}}' to terminate format key, found '{c}'").into(),
        ));
    }
    Ok(i)
}
pub fn parse_format_key(
    fmt: &[u8],
    start: usize,
    refs: &mut Vec<Option<String>>,
) -> Result<(FormatKey, usize), (usize, Cow<'static, str>)> {
    debug_assert!(fmt[start] == '{' as u8);
    let mut i = start + 1;
    let mut key = FormatKey::default();
    if let Some(mut end) = (&fmt[i..]).find_byteset("}:") {
        end += i;
        let c0 = fmt[end] as char;
        let ref_val = if end > i {
            Some(
                (&fmt[i..end])
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
        key.identifier = refs.len() - 1;
        i = end;
        if c0 == ':' {
            i = parse_format_flags(fmt, i, &mut key, refs)?;
        }
        return Ok((key, i + 1));
    }
    return Err((fmt.len(), NO_CLOSING_BRACE_ERR));
}

pub fn parse_format_string(
    fmt: &[u8],
    refs: &mut Vec<Option<String>>,
) -> Result<Vec<FormatPart>, (usize, Cow<'static, str>)> {
    let mut parts = Vec::new();
    let mut pending_literal = Vec::default();
    let mut i = 0;
    loop {
        let non_braced_begin = (&fmt[i..]).find_byteset("{}");
        if let Some(mut nbb) = non_braced_begin {
            nbb += i;
            if fmt[nbb] == '}' as u8 {
                if fmt[nbb + 1] != '}' as u8 {
                    return Err((nbb, "unmatched '}', consider using '}}'".into()));
                }
                pending_literal.extend_from_slice(&fmt[i..nbb + 1]);
                i = nbb + 2;
                continue;
            }
            if fmt[nbb + 1] == '{' as u8 {
                pending_literal.extend_from_slice(&fmt[i..nbb + 1]);
                i = nbb + 2;
                continue;
            }
            pending_literal.extend_from_slice(&fmt[i..nbb]);
            i = nbb;
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
                pending_literal = Default::default();
            }
            let (key, end) = parse_format_key(fmt, i, refs)?;
            parts.push(FormatPart::Key(key));
            i = end;
        } else {
            pending_literal.extend_from_slice(&fmt[i..]);
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
            }
            return Ok(parts);
        }
    }
}

pub fn parse_op_format(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let val = value.ok_or_else(|| {
        OperatorCreationError::new("missing argument for the regex operator", arg_idx)
    })?;
    let mut refs_str = Vec::new();
    let parts =
        parse_format_string(val, &mut refs_str).map_err(|(i, msg)| OperatorCreationError {
            message: format!("format string index {}: {}", i, msg).into(),
            cli_arg_idx: arg_idx,
        })?;
    let refs_idx = Vec::with_capacity(refs_str.len());
    Ok(OperatorData::Format(OpFormat {
        parts,
        refs_str,
        refs_idx,
    }))
}
pub fn create_op_format(val: &[u8]) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val), None)
}
pub fn create_op_format_from_str(val: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val.as_bytes()), None)
}
fn iter_output_states(
    fmt: &mut TfFormat,
    output_idx: &mut usize,
    run_len: RunLength,
    func: impl FnMut(&mut OutputState),
) {
    iter_output_states_advanced(&mut fmt.output_states, output_idx, run_len as usize, func);
}
fn iter_output_states_advanced(
    output_states: &mut Vec<OutputState>,
    output_idx: &mut usize,
    mut run_len: usize,
    mut func: impl FnMut(&mut OutputState),
) {
    if run_len == 0 {
        return;
    }
    let next = output_states.len();
    let o = &mut output_states[*output_idx];
    if run_len < o.run_len {
        let mut o2 = *o;
        o.next = next;
        let rl_rem = o.run_len - run_len;
        o.run_len = run_len;
        o2.run_len = rl_rem;
        output_states.push(o2);
    }
    while run_len > 0 {
        let o = &mut output_states[*output_idx];
        if !o.error_occured && o.incomplete_stream_value_handle.is_none() {
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
    func: impl Fn(&mut OutputTarget),
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
fn calc_text_len(
    k: &FormatKey,
    text_len: usize,
    width_lookup: usize,
    text_char_count: &mut impl ValueProducingCallable<usize>,
) -> usize {
    let max_width = match k.width {
        Some(FormatWidthSpec::Ref(_)) => width_lookup,
        Some(FormatWidthSpec::Value(v)) => v,
        None => 0,
    };
    if (text_len / MAX_UTF8_CHAR_LEN) >= max_width {
        text_len
    } else {
        let char_count = text_char_count.call();
        if char_count >= max_width {
            text_len
        } else {
            text_len
                + (max_width - char_count)
                    * k.fill
                        .as_ref()
                        .map(|f| f.fill_char.map(|c| c.len_utf8()).unwrap_or(1))
                        .unwrap_or(1)
        }
    }
}
fn calc_text_padding(
    k: &FormatKey,
    text_len: usize,
    width_lookup: usize,
    text_char_count: impl FnOnce() -> usize,
) -> usize {
    let max_width = match k.width {
        Some(FormatWidthSpec::Ref(_)) => width_lookup,
        Some(FormatWidthSpec::Value(v)) => v,
        None => 0,
    };
    if (text_len >> MAX_UTF8_CHAR_LEN) >= max_width {
        0
    } else {
        let char_count = text_char_count();
        if char_count >= max_width {
            0
        } else {
            max_width - char_count
        }
    }
}
pub fn lookup_widths(
    field_mgr: &FieldManager,
    match_set_mgr: &mut MatchSetManager,
    fmt: &mut TfFormat,
    k: &FormatKey,
    batch_size: usize,
    unconsumed_input: bool,
    apply_actions: bool,
    update_iter: bool,
    succ_func: impl Fn(&mut TfFormat, &mut usize, usize, usize), //output idx, width, run length
    err_func: impl Fn(&mut TfFormat, &mut usize, usize),         //output idx, width, run length
) {
    let ident_ref = if let Some(FormatWidthSpec::Ref(ident)) = k.width {
        fmt.refs[ident]
    } else {
        return;
    };
    if apply_actions {
        field_mgr.apply_field_actions(match_set_mgr, ident_ref.field_id);
    }
    let field = field_mgr.borrow_field_cow(ident_ref.field_id, unconsumed_input);
    let mut iter = field_mgr
        .get_iter_cow_aware(ident_ref.field_id, &field, ident_ref.iter_id)
        .bounded(0, batch_size);
    let mut output_index = 0;
    let mut handled_fields = 0;
    while let Some(range) = iter.typed_range_fwd(usize::MAX, 0) {
        match range.data {
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range, ints) {
                    let width = if *v < 0 { 0 } else { *v as usize };
                    succ_func(fmt, &mut output_index, width, rl as usize);
                }
            }
            _ => err_func(fmt, &mut output_index, range.field_count),
        }
        handled_fields += range.field_count;
    }
    iter_output_states_advanced(
        &mut fmt.output_states,
        &mut output_index,
        batch_size - handled_fields,
        |os| os.error_occured = true,
    );
    if update_iter {
        field_mgr.store_iter_cow_aware(ident_ref.field_id, &field, ident_ref.iter_id, iter);
    }
}
pub fn setup_key_output_state(
    sv_mgr: &mut StreamValueManager,
    field_mgr: &FieldManager,
    match_set_mgr: &mut MatchSetManager,
    tf_id: TransformId,
    part_idx: usize,
    fmt: &mut TfFormat,
    batch_size: usize,
    unconsumed_input: bool,
    k: &FormatKey,
) {
    lookup_widths(
        field_mgr,
        match_set_mgr,
        fmt,
        k,
        batch_size,
        unconsumed_input,
        true,
        false,
        |fmt, output_idx, width, run_len| {
            iter_output_states_advanced(&mut fmt.output_states, output_idx, run_len, |os| {
                os.width_lookup = width
            })
        },
        |fmt, output_idx, run_len| {
            iter_output_states_advanced(&mut fmt.output_states, output_idx, run_len, |os| {
                os.error_occured = true
            })
        },
    );
    let ident_ref = fmt.refs[k.identifier];
    field_mgr.apply_field_actions(match_set_mgr, ident_ref.field_id);
    let field = field_mgr.borrow_field_cow(ident_ref.field_id, unconsumed_input);
    let mut iter = AutoDerefIter::new(
        field_mgr,
        ident_ref.field_id,
        field_mgr
            .get_iter_cow_aware(ident_ref.field_id, &field, ident_ref.iter_id)
            .bounded(0, batch_size),
    );

    let mut output_index = 0;
    let mut handled_fields = 0;
    let debug_format = [FormatType::Debug, FormatType::MoreDebug].contains(&k.format_type);
    while let Some(range) =
        iter.typed_range_fwd(match_set_mgr, usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                        if debug_format {
                            o.len += 2;
                        }
                    });
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                        o.contains_raw_bytes = true;
                        if debug_format {
                            o.len += 2;
                        }
                    });
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                        o.contains_raw_bytes = true;
                        if debug_format {
                            o.len += 3;
                        }
                    });
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let digits = i64_digits(k.add_plus_sign, *v);
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.len += calc_text_len(k, digits, o.width_lookup, &mut || digits);
                    });
                }
            }
            TypedSlice::StreamValueId(svs) => {
                for (v, range, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    let sv = &mut sv_mgr.stream_values[v];

                    match &sv.data {
                        StreamValueData::Dropped => unreachable!(),
                        StreamValueData::Error(e) => {
                            if debug_format {
                                iter_output_states(fmt, &mut output_index, rl, |o| {
                                    let (len, mut char_count) = formatted_error_string_len(
                                        e,
                                        k.format_type,
                                        k.alternate_form,
                                        true,
                                    );
                                    o.len += calc_text_len(k, len, o.width_lookup, &mut char_count);
                                });
                            } else {
                                iter_output_states(fmt, &mut output_index, rl, |o| {
                                    o.error_occured = true;
                                });
                            }
                        }
                        StreamValueData::Bytes(b) => {
                            let mut complete = sv.done;
                            let mut data = b.as_slice();
                            let mut idx_end = None;
                            if let Some(r) = range {
                                data = &data[r];
                                complete = true;
                            }
                            let debug_add_len = match k.format_type {
                                FormatType::Debug => 2,
                                FormatType::MoreDebug => 3,
                                _ => 0,
                            };
                            let mut char_count = cached!(data.chars().count() + debug_add_len);
                            let text_len = data.len() + debug_add_len;
                            let mut need_buffer = false;
                            if !complete && !sv.is_buffered() {
                                if let Some(width_spec) = &k.width {
                                    let mut i = output_index;
                                    iter_output_states(fmt, &mut i, rl, |o| {
                                        if width_spec.width(o.width_lookup)
                                            > data.len() / MAX_UTF8_CHAR_LEN
                                        {
                                            need_buffer = true;
                                        }
                                    });
                                    idx_end = Some(i);
                                }
                            }
                            if complete || !need_buffer {
                                let mut i = output_index;

                                iter_output_states(fmt, &mut i, rl, |o| {
                                    o.len +=
                                        calc_text_len(k, text_len, o.width_lookup, &mut char_count);
                                    if sv.bytes_are_utf8 {
                                        o.contains_raw_bytes = true;
                                    }
                                });
                                idx_end = Some(i);
                            }

                            if need_buffer {
                                sv.promote_to_buffer();
                            }
                            if !complete {
                                let mut i = output_index;

                                iter_output_states_advanced(
                                    &mut fmt.output_states,
                                    &mut i,
                                    rl as usize,
                                    |o| {
                                        sv_mgr.stream_values[v].subscribe(
                                            tf_id,
                                            fmt.stream_value_handles.peek_claim_id().into(),
                                            need_buffer,
                                        );
                                        let target_sv_id =
                                            sv_mgr.stream_values.claim_with_value(StreamValue {
                                                data: StreamValueData::Bytes(Vec::new()),
                                                bytes_are_utf8: true,
                                                bytes_are_chunk: true,
                                                drop_previous_chunks: false,
                                                done: false,
                                                subscribers: Default::default(),
                                                ref_count: 1,
                                            });
                                        o.incomplete_stream_value_handle =
                                            Some(fmt.stream_value_handles.claim_with_value(
                                                TfFormatStreamValueHandle {
                                                    part_idx,
                                                    target_sv_id,
                                                    handled_len: 0,
                                                    width_lookup: o.width_lookup,
                                                    wait_to_end: need_buffer,
                                                },
                                            ));
                                    },
                                );

                                idx_end = Some(i);
                            }

                            output_index = idx_end.unwrap();
                        }
                    }
                }
            }
            TypedSlice::Null(_) | TypedSlice::Success(_) if debug_format => {
                let len = typed_slice_zst_str(&range.base.data).len();
                iter_output_states_advanced(
                    &mut fmt.output_states,
                    &mut output_index,
                    range.base.field_count,
                    |o| {
                        o.len += calc_text_len(k, len, o.width_lookup, &mut || len);
                    },
                );
            }
            TypedSlice::Error(errs) if debug_format => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    let (len, mut char_count) =
                        formatted_error_string_len(v, k.format_type, k.alternate_form, false);
                    let mut cc = cached!(char_count.call());
                    iter_output_states(fmt, &mut output_index, rl, |o| {
                        o.len += calc_text_len(k, len, o.width_lookup, &mut cc);
                    });
                }
            }
            TypedSlice::Success(_) | TypedSlice::Null(_) | TypedSlice::Error(_) => {
                debug_assert!(!k.alternate_form);
                iter_output_states_advanced(
                    &mut fmt.output_states,
                    &mut output_index,
                    range.base.field_count,
                    |o| {
                        o.error_occured = true;
                    },
                );
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                todo!();
            }
        }
        handled_fields += range.base.field_count;
    }
    let uninitialized_fields = batch_size - handled_fields;
    iter_output_states_advanced(
        &mut fmt.output_states,
        &mut output_index,
        uninitialized_fields,
        |o| {
            if debug_format {
                o.len += calc_text_len(k, NULL_STR.len(), o.width_lookup, &mut || NULL_STR.len());
            } else {
                o.error_occured = true
            }
        },
    );
    // we don't store the iter state back here because we need to iterate a second time
    // for the actual write
}
unsafe fn write_bytes_to_target(tgt: &mut OutputTarget, bytes: &[u8]) {
    unsafe {
        let ptr = tgt.target.unwrap().as_ptr();
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
        tgt.target = Some(NonNull::new_unchecked(ptr.add(bytes.len())));
    }
}
unsafe fn write_padding_to_tgt(tgt: &mut OutputTarget, fill_char: Option<char>, mut len: usize) {
    if len == 0 {
        return;
    }
    let mut char_enc = [0 as u8; MAX_UTF8_CHAR_LEN];
    let char_slice = fill_char.unwrap_or(' ').encode_utf8(&mut char_enc);
    let mut buf = ArrayVec::<u8, 32>::new();
    let chars_cap = divide_by_char_len(buf.capacity(), char_slice.len());
    for _ in 0..chars_cap.min(len) {
        buf.extend(char_slice.as_bytes().iter().cloned());
    }
    unsafe {
        while len > chars_cap {
            write_bytes_to_target(tgt, buf.as_slice());
            len -= chars_cap;
        }
        write_bytes_to_target(tgt, &buf.as_slice()[0..len * char_slice.len()]);
    }
}
fn setup_output_targets(
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

    let starting_len = unsafe { output_field.field_data.internals().data.len() };
    let mut tgt_len = starting_len;
    for os in fmt.output_states.iter() {
        if os.error_occured {
            tgt_len = FieldValueKind::Error.align_size_up(tgt_len);
            tgt_len += FieldValueKind::Error.size();
        } else if os.incomplete_stream_value_handle.is_some() {
            tgt_len = FieldValueKind::StreamValueId.align_size_up(tgt_len);
            tgt_len += FieldValueKind::StreamValueId.size();
        } else if os.len <= INLINE_STR_MAX_LEN {
            tgt_len += os.len;
        } else {
            tgt_len = FieldValueKind::BytesBuffer.align_size_up(tgt_len);
            tgt_len += FieldValueKind::BytesBuffer.size();
        }
    }
    unsafe {
        output_field
            .field_data
            .internals()
            .data
            .reserve(tgt_len - starting_len);
    }

    loop {
        let os = &mut fmt.output_states[output_idx];
        let target: Option<NonNull<u8>>;
        if os.error_occured {
            target = None;
            output_field.field_data.push_error(
                OperatorApplicationError::new("Format Error", op_id), //TODO: give more context
                os.run_len,
                true,
                false,
            );
        } else if let Some(handle_id) = os.incomplete_stream_value_handle {
            let handle = &fmt.stream_value_handles[handle_id];
            if let StreamValueData::Bytes(buf) = &mut sv_mgr.stream_values[handle.target_sv_id].data
            {
                buf.reserve(os.len);
                unsafe {
                    target = Some(NonNull::new_unchecked(buf.as_mut_ptr()));
                }
                // just to be 'rust compliant' ...
                buf.extend(std::iter::repeat(0).take(os.len));
            } else {
                unreachable!();
            }
            output_field.field_data.push_stream_value_id(
                handle.target_sv_id,
                os.run_len,
                true,
                false,
            );
        } else if os.len <= INLINE_STR_MAX_LEN {
            let flags = if os.contains_raw_bytes {
                0
            } else {
                field_value_flags::BYTES_ARE_UTF8
            };
            unsafe {
                target = Some(NonNull::new_unchecked(
                    output_field.field_data.push_variable_sized_type_uninit(
                        FieldValueKind::BytesInline,
                        flags | field_value_flags::SHARED_VALUE,
                        os.len,
                        os.run_len,
                    ),
                ));
            }
        } else {
            let mut buf = Vec::with_capacity(os.len);
            unsafe {
                target = Some(NonNull::new_unchecked(buf.as_mut_ptr()));
            }
            // just to be 'rust compliant' ...
            buf.extend(std::iter::repeat(0).take(os.len));
            output_field
                .field_data
                .push_bytes_buffer(buf, os.run_len, true, false);
        };
        fmt.output_targets.push(OutputTarget {
            run_len: os.run_len,
            target,
            width_lookup: os.width_lookup,
        });
        output_idx = os.next;
        if output_idx == FINAL_OUTPUT_INDEX_NEXT_VAL {
            break;
        }
    }
}
#[inline(always)]
unsafe fn write_padded_bytes_with_prefix_suffix(
    k: &FormatKey,
    tgt: &mut OutputTarget,
    data: &[u8],
    prefix: &[u8],
    suffix: &[u8],
) {
    if k.width.is_some() {
        let fill_spec = k.fill.as_ref().cloned().unwrap_or_default();
        let padding = calc_text_padding(
            k,
            data.len() + prefix.len() + suffix.len(),
            tgt.width_lookup,
            || data.chars().count() + prefix.chars().count() + suffix.chars().count(),
        );
        let (pad_left, pad_right) = match fill_spec.alignment {
            FormatFillAlignment::Left => (padding, 0),
            FormatFillAlignment::Center => ((padding + 1) / 2, padding / 2),
            FormatFillAlignment::Right => (0, padding),
        };
        write_padding_to_tgt(tgt, fill_spec.fill_char, pad_left);
        write_bytes_to_target(tgt, prefix);
        write_bytes_to_target(tgt, data);
        write_bytes_to_target(tgt, suffix);
        write_padding_to_tgt(tgt, fill_spec.fill_char, pad_right);
    } else {
        write_bytes_to_target(tgt, prefix);
        write_bytes_to_target(tgt, data);
        write_bytes_to_target(tgt, suffix);
    }
}
unsafe fn write_padded_bytes(k: &FormatKey, tgt: &mut OutputTarget, data: &[u8]) {
    write_padded_bytes_with_prefix_suffix(k, tgt, data, &[], &[]);
}
unsafe fn write_formatted_int(k: &FormatKey, tgt: &mut OutputTarget, value: i64) {
    if !k.width.is_some() {
        write_bytes_to_target(tgt, i64_to_str(k.add_plus_sign, value).as_bytes());
        return;
    }
    if !k.zero_pad_numbers {
        let val = i64_to_str(k.add_plus_sign, value);
        write_padded_bytes(k, tgt, val.as_bytes());
        return;
    }
    let mut len = 0;
    if value < 0 {
        write_bytes_to_target(tgt, "-".as_bytes());
        len += 1;
    } else if k.add_plus_sign {
        write_bytes_to_target(tgt, "+".as_bytes());
        len += 1;
    }
    let val = u64_to_str(false, value.unsigned_abs());
    len += val.len();
    let padding = calc_text_padding(k, len, tgt.width_lookup, || val.len());
    write_padding_to_tgt(tgt, Some('0'), padding);
    write_bytes_to_target(tgt, val.as_bytes());
}
fn error_to_formatted_string(
    e: &OperatorApplicationError,
    ft: FormatType,
    alternate_form: bool,
    stream_value: bool,
) -> String {
    match ft {
        FormatType::Debug => {
            if alternate_form {
                format!("!\"{}\"", e.message)
            } else {
                format!("!\"{ERROR_PREFIX_STR}{}\"", e)
            }
        }
        FormatType::MoreDebug => {
            let sv = if stream_value { "~" } else { "" };
            if alternate_form {
                format!("{sv}!\"{}\"", e.message)
            } else {
                format!("{sv}!\"{ERROR_PREFIX_STR}{}\"", e)
            }
        }
        _ => unreachable!(),
    }
}
struct ErrLenCalculator<'a> {
    err: &'a OperatorApplicationError,
    additional_len: usize,
    alternate_form: bool,
}
impl<'a> ValueProducingCallable<usize> for ErrLenCalculator<'a> {
    fn call(&mut self) -> usize {
        self.additional_len
            + if self.alternate_form {
                self.err.message.chars().count()
            } else {
                let mut cw = LengthAndCharsCountingWriter::default();
                cw.write_fmt(format_args!("{ERROR_PREFIX_STR}{}", self.err))
                    .unwrap();
                cw.char_count
            }
    }
}
fn formatted_error_string_len<'a>(
    e: &'a OperatorApplicationError,
    ft: FormatType,
    alternate_form: bool,
    stream_value: bool,
) -> (usize, ErrLenCalculator) {
    let additional_len = match ft {
        FormatType::Debug => 2 + 1, // !"..."
        FormatType::MoreDebug => (if stream_value { 1 } else { 0 }) + 2 + 1, //~!"..." vs !"..."
        _ => unreachable!(),
    };
    let len = if alternate_form {
        e.message.len()
    } else {
        let mut cw = LengthCountingWriter::default();
        cw.write_fmt(format_args!("{ERROR_PREFIX_STR}{e}")).unwrap();
        cw.len
    };
    (
        len + additional_len,
        ErrLenCalculator {
            additional_len,
            alternate_form,
            err: e,
        },
    )
}
fn write_fmt_key(
    sv_mgr: &mut StreamValueManager,
    field_mgr: &FieldManager,
    match_set_mgr: &mut MatchSetManager,
    fmt: &mut TfFormat,
    batch_size: usize,
    k: &FormatKey,
) {
    //any potential unconsumed input was already set during width calculation
    let unconsumed_input = false;
    lookup_widths(
        field_mgr,
        match_set_mgr,
        fmt,
        k,
        batch_size,
        unconsumed_input,
        false,
        true,
        |fmt, output_idx, width, run_len| {
            iter_output_targets(fmt, output_idx, run_len, |ot| ot.width_lookup = width)
        },
        |_fmt, _output_idx, _run_len| (),
    );
    let ident_ref = fmt.refs[k.identifier];

    let field = field_mgr.borrow_field_cow(ident_ref.field_id, unconsumed_input);
    let base_iter = field_mgr
        .get_iter_cow_aware(ident_ref.field_id, &field, ident_ref.iter_id)
        .bounded(0, batch_size);
    let field_pos_start = base_iter.get_next_field_pos();
    let mut iter = AutoDerefIter::new(field_mgr, ident_ref.field_id, base_iter);
    let debug_format = [FormatType::Debug, FormatType::MoreDebug].contains(&k.format_type);
    let mut output_index = 0;
    while let Some(range) =
        iter.typed_range_fwd(match_set_mgr, usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        //TODO: respect format options
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        if debug_format {
                            write_padded_bytes_with_prefix_suffix(
                                k,
                                tgt,
                                v.as_bytes(),
                                b"\"",
                                b"\"",
                            );
                        } else {
                            write_padded_bytes(k, tgt, v.as_bytes());
                        }
                    });
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        if debug_format {
                            write_padded_bytes_with_prefix_suffix(k, tgt, v, b"'", b"'");
                        } else {
                            write_padded_bytes(k, tgt, v);
                        }
                    });
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        if debug_format {
                            write_padded_bytes_with_prefix_suffix(k, tgt, v, b"'", b"'");
                        } else {
                            write_padded_bytes(k, tgt, v);
                        }
                    });
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        write_formatted_int(k, tgt, *v)
                    });
                }
            }
            TypedSlice::Null(_) | TypedSlice::Success(_) if debug_format => {
                let data = typed_slice_zst_str(&range.base.data).as_bytes();
                iter_output_targets(
                    fmt,
                    &mut output_index,
                    range.base.field_count,
                    |tgt| unsafe {
                        write_padded_bytes(k, tgt, data);
                    },
                );
            }
            TypedSlice::Error(errs) if debug_format => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    let err_str =
                        error_to_formatted_string(v, k.format_type, k.alternate_form, false);
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        write_padded_bytes(k, tgt, &err_str.as_bytes())
                    });
                }
            }

            TypedSlice::StreamValueId(svs) => {
                for (v, range, rl) in RefAwareStreamValueIter::from_range(&range, svs) {
                    let sv = &sv_mgr.stream_values[v];

                    match &sv.data {
                        StreamValueData::Dropped => unreachable!(),
                        StreamValueData::Error(e) => {
                            let err_str =
                                error_to_formatted_string(e, k.format_type, k.alternate_form, true);
                            iter_output_targets(
                                fmt,
                                &mut output_index,
                                rl as usize,
                                |tgt| unsafe { write_padded_bytes(k, tgt, &err_str.as_bytes()) },
                            );
                        }
                        StreamValueData::Bytes(b) => {
                            let data = range.as_ref().cloned().map(|r| &b[r]).unwrap_or(b);

                            if range.is_some() || sv.done || !sv.is_buffered() {
                                let qc = if sv.bytes_are_utf8 { '"' } else { '\'' };
                                let left = ['~' as u8, qc as u8];
                                let right = [qc as u8];
                                let none = b"".as_slice();
                                let (left, right) = match k.format_type {
                                    FormatType::Debug => (right.as_slice(), right.as_slice()),
                                    FormatType::MoreDebug => (left.as_slice(), right.as_slice()),
                                    _ => (none, none),
                                };
                                iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| {
                                    unsafe {
                                        write_padded_bytes_with_prefix_suffix(
                                            k, tgt, data, left, right,
                                        )
                                    }
                                    if !sv.done {
                                        tgt.target = None;
                                    }
                                });
                            } else {
                                iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| {
                                    tgt.target = None;
                                });
                            }
                        }
                    }
                }
            }
            TypedSlice::Success(_)
            | TypedSlice::Null(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::Object(_) => {
                // just to increase output index
                iter_output_targets(
                    fmt,
                    &mut output_index,
                    range.base.field_count,
                    |_tgt| unreachable!(),
                );
            }
        }
    }
    let base_iter = iter.into_base_iter();
    let field_pos_end = base_iter.get_next_field_pos();
    field_mgr.store_iter_cow_aware(ident_ref.field_id, &field, ident_ref.iter_id, base_iter);
    if debug_format {
        let unconsumed_fields = batch_size - (field_pos_end - field_pos_start);
        iter_output_targets(fmt, &mut output_index, unconsumed_fields, |tgt| unsafe {
            write_padded_bytes(k, tgt, NULL_STR.as_bytes());
        });
    }
}
pub fn handle_tf_format(sess: &mut JobData, tf_id: TransformId, fmt: &mut TfFormat) {
    let (batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    let mut output_field =
        sess.tf_mgr
            .prepare_output_field(&sess.field_mgr, &mut sess.match_set_mgr, tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let unconsumed_input = tf.has_unconsumed_input();
    fmt.output_states.push(OutputState {
        run_len: batch_size,
        next: FINAL_OUTPUT_INDEX_NEXT_VAL,
        len: 0,
        width_lookup: 0,
        contains_raw_bytes: false,
        error_occured: false,
        incomplete_stream_value_handle: None,
    });
    for (part_idx, part) in fmt.parts.iter().enumerate() {
        match part {
            FormatPart::ByteLiteral(v) => fmt.output_states.iter_mut().for_each(|s| {
                if s.incomplete_stream_value_handle.is_none() {
                    s.len += v.len();
                    s.contains_raw_bytes = true;
                }
            }),
            FormatPart::TextLiteral(v) => {
                fmt.output_states.iter_mut().for_each(|s| {
                    if s.incomplete_stream_value_handle.is_none() {
                        s.len += v.len()
                    }
                });
            }
            FormatPart::Key(k) => setup_key_output_state(
                &mut sess.sv_mgr,
                &sess.field_mgr,
                &mut sess.match_set_mgr,
                tf_id,
                part_idx,
                fmt,
                batch_size,
                unconsumed_input,
                k,
            ),
        }
    }
    setup_output_targets(fmt, &mut sess.sv_mgr, tf.op_id.unwrap(), &mut output_field);
    for part in fmt.parts.iter() {
        match part {
            FormatPart::ByteLiteral(v) => {
                iter_output_targets(fmt, &mut 0, batch_size, |tgt| unsafe {
                    write_bytes_to_target(tgt, v)
                });
            }
            FormatPart::TextLiteral(v) => {
                iter_output_targets(fmt, &mut 0, batch_size, |tgt| unsafe {
                    write_bytes_to_target(tgt, v.as_bytes())
                });
            }
            FormatPart::Key(k) => write_fmt_key(
                &mut sess.sv_mgr,
                &sess.field_mgr,
                &mut sess.match_set_mgr,
                fmt,
                batch_size,
                k,
            ),
        }
    }
    fmt.output_states.clear();
    fmt.output_targets.clear();
    drop(output_field);
    if input_done {
        for r in &fmt.refs {
            sess.drop_field_refcount(r.field_id);
        }
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}

pub fn handle_tf_format_stream_value_update(
    sess: &mut JobData,
    tf_id: TransformId,
    tf: &mut TfFormat,
    sv_id: StreamValueId,
    custom: usize,
) {
    let handle_id = NonMaxUsize::new(custom).unwrap();
    let handle = &mut tf.stream_value_handles[handle_id];
    let (sv, out_sv) = sess
        .sv_mgr
        .stream_values
        .get_two_distinct_mut(sv_id, handle.target_sv_id);
    let (sv, mut out_sv) = (sv.unwrap(), out_sv.unwrap());
    let done = sv.done;
    match &sv.data {
        StreamValueData::Error(err) => {
            debug_assert!(sv.done);
            out_sv.data = StreamValueData::Error(err.clone());
        }
        StreamValueData::Bytes(data) => match &mut out_sv.data {
            StreamValueData::Dropped => unreachable!(),
            StreamValueData::Error(_) => unreachable!(),
            StreamValueData::Bytes(tgt_buf) => {
                if !sv.bytes_are_chunk {
                    if !handle.wait_to_end {
                        if sv.done {
                            tgt_buf.extend(&data[handle.handled_len..]);
                        } else {
                            //TODO: change the subscription type
                        }
                    }
                    if sv.done {
                        if let FormatPart::Key(k) = &tf.parts[handle.part_idx] {
                            let len =
                                calc_text_len(k, data.len(), handle.width_lookup, &mut || {
                                    data.chars().count()
                                });
                            tgt_buf.reserve(len);
                            unsafe {
                                //TODO: this is a hack, create a separate impl
                                let mut output_target = OutputTarget {
                                    run_len: 1,
                                    width_lookup: handle.width_lookup,
                                    target: Some(NonNull::new_unchecked(
                                        tgt_buf.as_mut_ptr_range().end,
                                    )),
                                };
                                write_padded_bytes(k, &mut output_target, data);
                            };
                        } else {
                            unreachable!();
                        }
                    }
                } else {
                    out_sv.drop_previous_chunks = sv.drop_previous_chunks;
                    if out_sv.bytes_are_chunk || sv.drop_previous_chunks {
                        tgt_buf.clear();
                    }
                    if sv.drop_previous_chunks {
                        handle.handled_len = 0;
                    }
                    handle.handled_len += data.len();
                    tgt_buf.extend(data);
                    sess.sv_mgr
                        .inform_stream_value_subscribers(handle.target_sv_id);
                }
            }
        },
        StreamValueData::Dropped => unreachable!(),
    }
    if !done {
        return;
    }

    sess.sv_mgr
        .drop_field_value_subscription(sv_id, Some(tf_id));
    out_sv = &mut sess.sv_mgr.stream_values[handle.target_sv_id];
    let mut i = handle.part_idx + 1;
    if let StreamValueData::Bytes(bb) = &mut out_sv.data {
        while i < tf.parts.len() {
            match &tf.parts[i] {
                FormatPart::ByteLiteral(l) => {
                    bb.extend_from_slice(l);
                    out_sv.bytes_are_utf8 = false;
                }
                FormatPart::TextLiteral(l) => {
                    bb.extend_from_slice(l.as_bytes());
                }
                FormatPart::Key(_k) => {
                    todo!();
                }
            }
            i += 1;
        }
        handle.part_idx = i;
    } else {
        unreachable!();
    }
    out_sv.done = true;
    sess.sv_mgr
        .drop_field_value_subscription(handle.target_sv_id, None);
    tf.stream_value_handles.release(handle_id);
    if !tf.stream_value_handles.is_empty() {
        sess.tf_mgr.update_ready_state(tf_id);
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use crate::operators::format::{
        FormatFillAlignment, FormatFillSpec, FormatKey, FormatWidthSpec,
    };

    use super::{parse_format_string, FormatPart};

    #[test]
    fn empty_format_string() {
        let mut dummy = Default::default();
        assert_eq!(parse_format_string(&[], &mut dummy).unwrap(), &[]);
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
            let mut dummy = Default::default();
            assert_eq!(
                parse_format_string(lit.as_bytes(), &mut dummy).unwrap(),
                &[FormatPart::TextLiteral(res.to_owned())]
            );
        }
    }

    #[test]
    fn two_keys() {
        let mut idents = Default::default();
        let mut a = FormatKey::default();
        a.identifier = 0;
        let mut b = FormatKey::default();
        b.identifier = 1;
        assert_eq!(
            parse_format_string("foo{{{a}}}__{b}".as_bytes(), &mut idents).unwrap(),
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
    fn fill_options() {
        let mut idents = Default::default();
        let mut a = FormatKey::default();
        a.identifier = 0;
        a.width = Some(FormatWidthSpec::Value(5));
        a.fill = Some(FormatFillSpec::new(Some('+'), FormatFillAlignment::Left));
        assert_eq!(
            parse_format_string("{a:+<5}".as_bytes(), &mut idents).unwrap(),
            &[FormatPart::Key(a),]
        );
        assert_eq!(idents, &[Some("a".to_owned())])
    }

    #[test]
    fn float_precision() {
        let mut idents = Default::default();
        let mut a = FormatKey::default();
        a.identifier = 0;
        a.width = Some(FormatWidthSpec::Value(3));
        a.float_precision = Some(FormatWidthSpec::Ref(1));
        assert_eq!(
            parse_format_string("{a:3.b$}".as_bytes(), &mut idents).unwrap(),
            &[FormatPart::Key(a)]
        );
        assert_eq!(idents, &[Some("a".to_owned()), Some("b".to_owned())])
    }

    #[test]
    fn width_not_an_ident() {
        let mut idents = Default::default();
        assert_eq!(
            parse_format_string("{a:1x$}".as_bytes(), &mut idents),
            Err((4, Cow::Borrowed("expected '?' after type specifier 'x'"))) //TODO: better error message for this case
        );
    }

    #[test]
    fn fill_char_is_optional_not_an_ident() {
        let mut idents = Default::default();
        let mut a = FormatKey::default();
        a.width = Some(FormatWidthSpec::Value(2));
        a.fill = Some(FormatFillSpec::new(None, FormatFillAlignment::Center));
        assert_eq!(
            parse_format_string("{a:^2}".as_bytes(), &mut idents).unwrap(),
            &[FormatPart::Key(a)]
        );
    }
}
