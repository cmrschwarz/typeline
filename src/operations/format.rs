use arrayvec::ArrayVec;
use bstr::{BStr, BString, ByteSlice, ByteVec};
use std::{
    borrow::Cow,
    cell::{RefCell, RefMut},
    ptr::NonNull,
};

use smallstr::SmallString;

use crate::{
    field_data::{
        field_value_flags,
        iter_hall::IterId,
        iters::FieldIterator,
        push_interface::{PushInterface, RawPushInterface},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
        FieldValueKind, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
        RefAwareStreamValueIter,
    },
    stream_value::{StreamValueData, StreamValueId},
    utils::{
        divide_by_char_len, i64_digits, i64_to_str,
        string_store::{StringStore, StringStoreEntry},
        u64_to_str,
        universe::Universe,
        ValueProducingCallable, MAX_UTF8_CHAR_LEN,
    },
    worker_thread_session::{
        Field, FieldId, JobData, MatchSet, MatchSetId, RecordManager, StreamValueManager,
    },
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
    print::{error_to_string, ERROR_PREFIX_STR, NULL_STR, SUCCESS_STR, UNSET_STR},
    transform::{TransformData, TransformId, TransformState},
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

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq, Default)]
pub enum NumberFormat {
    #[default]
    Plain, // the default value representation
    Binary,   // print integers with base 2, e.g 101010 instead of 42
    Octal,    // print integers with base 8, e.g 52 instead of 42
    Hex,      // print integers in lower case hexadecimal, e.g 2a instead of 42
    UpperHex, // print integers in upper case hexadecimal, e.g 2A instead of 42
    LowerExp, // print numbers in upper case scientific notation, e.g. 4.2e1 instead of 42
    UpperExp, // print numbers in lower case scientific notation, e.g. 4.2E1 instead of 42
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct FormatKey {
    identifier: FormatKeyRefId,
    fill: Option<FormatFillSpec>,
    add_plus_sign: bool,
    zero_pad_numbers: bool,
    width: Option<FormatWidthSpec>,
    float_precision: Option<FormatWidthSpec>,
    alternate_form: bool, // prefix 0x for hex, 0o for octal and 0b for binary, pretty print objects / arrays
    number_format: NumberFormat,
    debug: bool,
    unicode: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FormatPart {
    ByteLiteral(BString),
    TextLiteral(String),
    Key(FormatKey),
}

type FormatKeyRefId = usize;

pub struct OpFormat {
    parts: Vec<FormatPart>,
    refs_str: Vec<Option<String>>,
    refs_idx: Vec<Option<StringStoreEntry>>,
}

#[derive(Clone, Copy)]
pub struct FormatIdentRef {
    field_id: FieldId,
    iter_id: IterId,
}

struct TfFormatStreamValueHandle {
    part_idx: usize,
    handled_len: usize,
    part_iter_id: IterId,
    target_sv_id: StreamValueId,
    wait_to_end: bool,
}
#[derive(Clone, Copy)]
struct OutputState {
    next: usize,
    len: usize,
    width_lookup: usize,
    run_len: usize,
    contains_raw_bytes: bool,
    error_occured: bool,
    unconsumable_stream_value: bool,
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
            unconsumable_stream_value: false,
            run_len: 0,
            width_lookup: 0,
        }
    }
}

pub struct TfFormat<'a> {
    output_field: FieldId,
    parts: &'a Vec<FormatPart>,
    refs: Vec<FormatIdentRef>,
    output_states: Vec<OutputState>,
    output_targets: Vec<OutputTarget>,
    stream_value_handles: Universe<usize, TfFormatStreamValueHandle>,
}

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
    op: &'a OpFormat,
    tf_state: &mut TransformState,
    tf_id: TransformId,
) -> (TransformData<'a>, FieldId) {
    //TODO: cache field indices...
    let output_field = sess.record_mgr.add_field(
        tf_state.match_set_id,
        sess.record_mgr.get_min_apf_idx(tf_state.input_field),
        None,
    );
    let refs: Vec<_> = op
        .refs_idx
        .iter()
        .map(|name| {
            let (field_id, iter_id) = if let Some(name) = name {
                let id = sess.record_mgr.match_sets[tf_state.match_set_id]
                    .field_name_map
                    .get(name)
                    .and_then(|fields| fields.back().cloned())
                    .unwrap_or_else(|| {
                        sess.record_mgr.add_field(
                            tf_state.match_set_id,
                            sess.record_mgr.get_min_apf_idx(tf_state.input_field),
                            Some(*name),
                        )
                    });
                let mut f = sess.record_mgr.fields[id].borrow_mut();
                f.added_as_placeholder_by_tf = Some(tf_id);
                (id, f.field_data.claim_iter())
            } else {
                let iter_id = sess.record_mgr.fields[tf_state.input_field]
                    .borrow_mut()
                    .field_data
                    .claim_iter();
                (tf_state.input_field, iter_id)
            };
            FormatIdentRef { field_id, iter_id }
        })
        .collect();
    let tf = TfFormat {
        output_field,
        parts: &op.parts,
        refs,
        output_states: Default::default(),
        output_targets: Default::default(),
        stream_value_handles: Default::default(),
    };
    (TransformData::Format(tf), output_field)
}

fn create_format_literal(fmt: BString) -> FormatPart {
    match String::try_from(fmt) {
        Ok(v) => FormatPart::TextLiteral(v),
        Err(err) => FormatPart::ByteLiteral(BString::from(err.into_vec())),
    }
}

const NO_CLOSING_BRACE_ERR: Cow<'static, str> = Cow::Borrowed("format key has no closing '}'");

pub fn parse_format_width_spec<const FOR_FLOAT_PREC: bool>(
    fmt: &BStr,
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
            let val = unsafe { (&fmt[start..i]).to_str_unchecked() };
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
    fmt: &BStr,
    start: usize,
    key: &mut FormatKey,
    refs: &mut Vec<Option<String>>,
) -> Result<usize, (usize, Cow<'static, str>)> {
    fn next(fmt: &BStr, i: usize) -> Result<char, (usize, Cow<'static, str>)> {
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

    if c == '?' {
        key.alternate_form = true;
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
            'x' => key.number_format = NumberFormat::Hex,
            'X' => key.number_format = NumberFormat::UpperHex,
            'o' => key.number_format = NumberFormat::Octal,
            'b' => key.number_format = NumberFormat::Binary,
            'e' => key.number_format = NumberFormat::LowerExp,
            'E' => key.number_format = NumberFormat::UpperExp,
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
    fmt: &BStr,
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
    fmt: &BStr,
    refs: &mut Vec<Option<String>>,
) -> Result<Vec<FormatPart>, (usize, Cow<'static, str>)> {
    let mut parts = Vec::new();
    let mut pending_literal = BString::default();
    let mut i = 0;
    loop {
        let non_braced_begin = (&fmt[i..]).find_byteset("{}");
        if let Some(mut nbb) = non_braced_begin {
            nbb += i;
            if fmt[nbb] == '}' as u8 {
                if fmt[nbb + 1] != '}' as u8 {
                    return Err((nbb, "unmatched '}', consider using '}}'".into()));
                }
                pending_literal.push_str(&fmt[i..nbb + 1]);
                i = nbb + 2;
                continue;
            }
            if fmt[nbb + 1] == '{' as u8 {
                pending_literal.push_str(&fmt[i..nbb + 1]);
                i = nbb + 2;
                continue;
            }
            pending_literal.push_str(&fmt[i..nbb]);
            i = nbb;
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
                pending_literal = Default::default();
            }
            let (key, end) = parse_format_key(fmt, i, refs)?;
            parts.push(FormatPart::Key(key));
            i = end;
        } else {
            pending_literal.push_str(&fmt[i..]);
            if !pending_literal.is_empty() {
                parts.push(create_format_literal(pending_literal));
            }
            return Ok(parts);
        }
    }
}

pub fn parse_op_format(
    value: Option<&BStr>,
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
pub fn create_op_format(val: &BStr) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val), None)
}
pub fn create_op_format_from_str(val: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val.as_bytes().as_bstr()), None)
}
fn iter_output_states(
    fmt: &mut TfFormat,
    output_idx: &mut usize,
    mut run_len: usize,
    mut func: impl FnMut(&mut OutputState),
) {
    if run_len == 0 {
        return;
    }
    let next = fmt.output_states.len();
    let o = &mut fmt.output_states[*output_idx];
    if run_len < o.run_len {
        let mut o2 = *o;
        o.next = next;
        let rl_rem = o.run_len - run_len;
        o.run_len = run_len;
        o2.run_len = rl_rem;
        fmt.output_states.push(o2);
    }
    while run_len > 0 {
        let o = &mut fmt.output_states[*output_idx];
        if !o.error_occured && !o.unconsumable_stream_value {
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
    fields: &Universe<FieldId, RefCell<Field>>,
    match_sets: &mut Universe<MatchSetId, MatchSet>,
    fmt: &mut TfFormat,
    k: &FormatKey,
    batch_size: usize,
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
        RecordManager::apply_field_actions(fields, match_sets, ident_ref.field_id);
    }
    let field = &mut fields[ident_ref.field_id].borrow();
    let mut iter = field
        .field_data
        .get_iter(ident_ref.iter_id)
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
    iter_output_states(fmt, &mut output_index, batch_size - handled_fields, |os| {
        os.error_occured = true
    });
    if update_iter {
        field.field_data.store_iter(ident_ref.iter_id, iter);
    }
}
pub fn setup_key_output_state(
    sv_mgr: &mut StreamValueManager,
    fields: &Universe<FieldId, RefCell<Field>>,
    match_sets: &mut Universe<MatchSetId, MatchSet>,
    tf_id: TransformId,
    part_idx: usize,
    fmt: &mut TfFormat,
    batch_size: usize,
    k: &FormatKey,
) {
    lookup_widths(
        fields,
        match_sets,
        fmt,
        k,
        batch_size,
        true,
        false,
        |fmt, output_idx, width, run_len| {
            iter_output_states(fmt, output_idx, run_len, |os| os.width_lookup = width)
        },
        |fmt, output_idx, run_len| {
            iter_output_states(fmt, output_idx, run_len, |os| os.error_occured = true)
        },
    );
    let ident_ref = fmt.refs[k.identifier];
    RecordManager::apply_field_actions(fields, match_sets, ident_ref.field_id);
    let field = &mut fields[ident_ref.field_id].borrow();

    let mut iter = AutoDerefIter::new(
        fields,
        match_sets,
        ident_ref.field_id,
        field
            .field_data
            .get_iter(ident_ref.iter_id)
            .bounded(0, batch_size),
        None,
    );

    let mut output_index = 0;
    let mut handled_fields = 0;
    while let Some(range) =
        iter.typed_range_fwd(match_sets, usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                    });
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                        o.contains_raw_bytes = true;
                    });
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    let mut chars_count = cached!(v.chars().count());
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, v.len(), o.width_lookup, &mut chars_count);
                        o.contains_raw_bytes = true;
                    });
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let digits = i64_digits(k.add_plus_sign, *v);
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
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
                            if k.alternate_form {
                                iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                                    o.len += calc_text_len(
                                        k,
                                        ERROR_PREFIX_STR.len() + e.message.len(),
                                        o.width_lookup,
                                        &mut || {
                                            ERROR_PREFIX_STR.chars().count()
                                                + e.message.chars().count()
                                        },
                                    );
                                });
                            } else {
                                iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                                    o.error_occured = true;
                                });
                            }
                        }
                        StreamValueData::Bytes(b) => {
                            let mut need_buffer = false;
                            let mut complete = sv.done;
                            let mut data = b.as_slice();
                            let mut idx_end = None;
                            if let Some(r) = range {
                                data = &data[r];
                                complete = true;
                            }
                            let mut char_count = cached!(data.chars().count());
                            if !complete && !sv.is_buffered() {
                                if let Some(width_spec) = &k.width {
                                    let mut i = output_index;
                                    iter_output_states(fmt, &mut i, rl as usize, |o| {
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
                                iter_output_states(fmt, &mut i, rl as usize, |o| {
                                    o.len += calc_text_len(
                                        k,
                                        data.len(),
                                        o.width_lookup,
                                        &mut char_count,
                                    );
                                    if sv.bytes_are_utf8 {
                                        o.contains_raw_bytes = true;
                                    }
                                });
                                idx_end = Some(i);
                            }
                            if need_buffer {
                                sv.promote_to_buffer();
                                let mut i = output_index;
                                iter_output_states(fmt, &mut i, rl as usize, |o| {
                                    o.unconsumable_stream_value = true;
                                });
                                idx_end = Some(i);
                            }

                            if !complete {
                                sv.subscribe(
                                    tf_id,
                                    fmt.stream_value_handles.claim_with(|| {
                                        TfFormatStreamValueHandle {
                                            part_idx,
                                            handled_len: 0,
                                            target_sv_id: 0,
                                            part_iter_id: 0,
                                            wait_to_end: need_buffer,
                                        }
                                    }),
                                    need_buffer,
                                );
                            }
                            output_index = idx_end.unwrap();
                        }
                    }
                }
            }
            TypedSlice::Unset(ints) if k.alternate_form => {
                for (_, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, UNSET_STR.len(), o.width_lookup, &mut || {
                            UNSET_STR.len()
                        });
                    });
                }
            }
            TypedSlice::Success(ints) if k.alternate_form => {
                for (_, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, SUCCESS_STR.len(), o.width_lookup, &mut || {
                            SUCCESS_STR.len()
                        });
                    });
                }
            }
            TypedSlice::Null(ints) if k.alternate_form => {
                for (_, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, NULL_STR.len(), o.width_lookup, &mut || {
                            NULL_STR.len()
                        });
                    });
                }
            }
            TypedSlice::Error(errs) if k.alternate_form => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    let len = ERROR_PREFIX_STR.len() + v.message.len();
                    let mut char_count =
                        cached!(ERROR_PREFIX_STR.len() + v.message.chars().count());
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += calc_text_len(k, len, o.width_lookup, &mut char_count);
                    });
                }
            }
            TypedSlice::Unset(_)
            | TypedSlice::Success(_)
            | TypedSlice::Null(_)
            | TypedSlice::Error(_) => {
                debug_assert!(!k.alternate_form);
                iter_output_states(fmt, &mut output_index, range.base.field_count, |o| {
                    o.error_occured = true;
                });
            }
            TypedSlice::Html(_) | TypedSlice::Object(_) => {
                iter_output_states(fmt, &mut output_index, range.base.field_count, |o| {
                    o.error_occured = true;
                });
            }
        }
        handled_fields += range.base.field_count;
    }
    let uninitialized_fields = batch_size - handled_fields;
    iter_output_states(fmt, &mut output_index, uninitialized_fields, |os| {
        os.error_occured = true
    });
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
    _batch_size: usize,
    op_id: OperatorId,
    output_field: &mut RefMut<Field>,
) {
    fmt.output_targets.reserve(fmt.output_states.len());
    let mut output_idx = 0;

    let starting_len = unsafe { output_field.field_data.internals().data.len() };
    let mut tgt_len = starting_len;
    for os in fmt.output_states.iter() {
        if os.unconsumable_stream_value {}
        if os.error_occured {
            tgt_len = FieldValueKind::Error.align_size_up(tgt_len);
            tgt_len += std::mem::size_of::<OperatorApplicationError>();
        } else if os.len <= INLINE_STR_MAX_LEN {
            tgt_len += os.len;
        } else {
            tgt_len = FieldValueKind::BytesBuffer.align_size_up(tgt_len);
            tgt_len += std::mem::size_of::<Vec<u8>>();
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
        if output_idx == 0 {
            break;
        }
    }
}

unsafe fn write_padded_bytes(k: &FormatKey, tgt: &mut OutputTarget, data: &[u8]) {
    if k.width.is_some() {
        let fill_spec = k.fill.as_ref().cloned().unwrap_or_default();
        let padding = calc_text_padding(k, data.len(), tgt.width_lookup, || data.chars().count());
        let (pad_left, pad_right) = match fill_spec.alignment {
            FormatFillAlignment::Left => (padding, 0),
            FormatFillAlignment::Center => ((padding + 1) / 2, padding / 2),
            FormatFillAlignment::Right => (0, padding),
        };
        write_padding_to_tgt(tgt, fill_spec.fill_char, pad_left);
        write_bytes_to_target(tgt, data);
        write_padding_to_tgt(tgt, fill_spec.fill_char, pad_right);
    } else {
        write_bytes_to_target(tgt, data);
    }
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
fn write_fmt_key(
    sv_mgr: &mut StreamValueManager,
    fields: &Universe<FieldId, RefCell<Field>>,
    match_sets: &mut Universe<MatchSetId, MatchSet>,
    fmt: &mut TfFormat,
    batch_size: usize,
    k: &FormatKey,
) {
    lookup_widths(
        fields,
        match_sets,
        fmt,
        k,
        batch_size,
        false,
        true,
        |fmt, output_idx, width, run_len| {
            iter_output_targets(fmt, output_idx, run_len, |ot| ot.width_lookup = width)
        },
        |_fmt, _output_idx, _run_len| (),
    );
    let ident_ref = fmt.refs[k.identifier];
    let field = &mut fields[ident_ref.field_id].borrow();
    let mut iter = AutoDerefIter::new(
        fields,
        match_sets,
        ident_ref.field_id,
        field
            .field_data
            .get_iter(ident_ref.iter_id)
            .bounded(0, batch_size),
        None,
    );

    let mut output_index = 0;
    while let Some(range) =
        iter.typed_range_fwd(match_sets, usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        //TODO: respect format options
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        write_padded_bytes(k, tgt, v.as_bytes());
                    });
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        write_padded_bytes(k, tgt, v);
                    });
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    iter_output_targets(fmt, &mut output_index, rl as usize, |tgt| unsafe {
                        write_padded_bytes(k, tgt, v);
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
            TypedSlice::Null(_) if k.alternate_form => {
                iter_output_targets(
                    fmt,
                    &mut output_index,
                    range.base.field_count,
                    |tgt| unsafe {
                        write_padded_bytes(k, tgt, NULL_STR.as_bytes());
                    },
                );
            }
            TypedSlice::Error(errs) if k.alternate_form => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, errs) {
                    let err_str = error_to_string(v);
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
                            let err_str = error_to_string(e);
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
                                iter_output_targets(
                                    fmt,
                                    &mut output_index,
                                    rl as usize,
                                    |tgt| unsafe { write_padded_bytes(k, tgt, data) },
                                );
                            }
                        }
                    }
                }
            }
            TypedSlice::Unset(_)
            | TypedSlice::Success(_)
            | TypedSlice::Null(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::Object(_) => {
                // just to increase output index
                iter_output_targets(fmt, &mut output_index, range.base.field_count, |_tgt| ());
            }
        }
    }
    field
        .field_data
        .store_iter(ident_ref.iter_id, iter.into_base_iter());
}
pub fn handle_tf_format(sess: &mut JobData<'_>, tf_id: TransformId, fmt: &mut TfFormat) {
    sess.prepare_for_output(tf_id, std::slice::from_ref(&fmt.output_field));
    let batch_size = sess.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let mut output_field = sess.record_mgr.fields[fmt.output_field].borrow_mut();
    fmt.output_states.push(OutputState {
        run_len: batch_size,
        ..Default::default()
    });
    for (part_idx, part) in fmt.parts.iter().enumerate() {
        match part {
            FormatPart::ByteLiteral(v) => fmt.output_states.iter_mut().for_each(|s| {
                s.len += v.len();
                s.contains_raw_bytes = true;
            }),
            FormatPart::TextLiteral(v) => {
                fmt.output_states.iter_mut().for_each(|s| s.len += v.len())
            }
            FormatPart::Key(k) => setup_key_output_state(
                &mut sess.sv_mgr,
                &sess.record_mgr.fields,
                &mut sess.record_mgr.match_sets,
                tf_id,
                part_idx,
                fmt,
                batch_size,
                k,
            ),
        }
    }
    setup_output_targets(fmt, batch_size, op_id, &mut output_field);
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
                &sess.record_mgr.fields,
                &mut sess.record_mgr.match_sets,
                fmt,
                batch_size,
                k,
            ),
        }
    }
    fmt.output_states.clear();
    fmt.output_targets.clear();
    sess.tf_mgr.update_ready_state(tf_id);
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, batch_size);
}

pub fn handle_tf_format_stream_value_update(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    tf: &mut TfFormat,
    sv_id: StreamValueId,
    custom: usize,
) {
    let handle = &mut tf.stream_value_handles[custom];
    let (sv, out_sv) = sess
        .sv_mgr
        .stream_values
        .get_two_distinct_mut(sv_id, handle.target_sv_id);
    let (sv, mut out_sv) = (sv.unwrap(), out_sv.unwrap());
    let mut done = false;
    match &sv.data {
        StreamValueData::Error(err) => {
            out_sv.data = StreamValueData::Error(err.clone());
            done = true;
        }
        StreamValueData::Bytes(b) => {
            match &mut out_sv.data {
                StreamValueData::Dropped => unreachable!(),
                StreamValueData::Error(_) => unreachable!(),
                StreamValueData::Bytes(bb) => {
                    bb.extend(b);
                }
            }
            if sv.done {
                done = true;
            }
        }
        StreamValueData::Dropped => unreachable!(),
    }
    if !done {
        return;
    }

    sess.sv_mgr
        .drop_field_value_subscription(sv_id, Some(tf_id));
    out_sv = &mut sess.sv_mgr.stream_values[handle.target_sv_id];
    let i = handle.part_idx;
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
                FormatPart::Key(k) => {
                    todo!();
                }
            }
        }
    } else {
        unreachable!();
    }

    tf.stream_value_handles.release(custom);
    if tf.stream_value_handles.claimed_entry_count() > 0 {
        sess.tf_mgr.update_ready_state(tf_id);
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use bstr::ByteSlice;

    use crate::operations::format::{
        FormatFillAlignment, FormatFillSpec, FormatKey, FormatWidthSpec,
    };

    use super::{parse_format_string, FormatPart};

    #[test]
    fn empty_format_string() {
        let mut dummy = Default::default();
        assert_eq!(parse_format_string(&[].as_bstr(), &mut dummy).unwrap(), &[]);
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
                parse_format_string(lit.as_bytes().as_bstr(), &mut dummy).unwrap(),
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
            parse_format_string("foo{{{a}}}__{b}".as_bytes().as_bstr(), &mut idents).unwrap(),
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
            parse_format_string("{a:+<5}".as_bytes().as_bstr(), &mut idents).unwrap(),
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
            parse_format_string("{a:3.b$}".as_bytes().as_bstr(), &mut idents).unwrap(),
            &[FormatPart::Key(a)]
        );
        assert_eq!(idents, &[Some("a".to_owned()), Some("b".to_owned())])
    }

    #[test]
    fn width_not_an_ident() {
        let mut idents = Default::default();
        assert_eq!(
            parse_format_string("{a:1x$}".as_bytes().as_bstr(), &mut idents),
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
            parse_format_string("{a:^2}".as_bytes().as_bstr(), &mut idents).unwrap(),
            &[FormatPart::Key(a)]
        );
    }
}
