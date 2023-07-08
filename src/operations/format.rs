use std::{
    borrow::Cow,
    cell::{RefCell, RefMut},
    ptr::NonNull,
};

use bstr::{BStr, BString, ByteSlice, ByteVec};
use nonmax::NonMaxUsize;
use smallstr::SmallString;

use crate::{
    field_data::{
        field_value_flags,
        iter_hall::IterId,
        push_interface::{PushInterface, UnsafeHeaderPushInterface},
        typed::TypedSlice,
        typed_iters::TypedSliceIter,
        FieldValueFormat, FieldValueKind, FieldValueSize, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    ref_iter::{
        AutoDerefIter, RefAwareBytesBufferIter, RefAwareInlineBytesIter, RefAwareInlineTextIter,
    },
    stream_value::StreamValueId,
    utils::{
        i64_digits, i64_to_str,
        string_store::{StringStore, StringStoreEntry},
        universe::Universe,
    },
    worker_thread_session::{Field, FieldId, JobData, MatchSet, MatchSetId},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
    transform::{TransformData, TransformId, TransformState},
};

#[derive(Debug, PartialEq, Eq, Default)]
pub enum FormatFillAlignment {
    #[default]
    Left,
    Center,
    Right,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FormatWidthSpec {
    Value(usize),
    Ref(FormatKeyRefId),
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
    fill: Option<(char, FormatFillAlignment)>,
    add_plus_sign: bool,
    zero_pad_numbers: bool,
    width: FormatWidthSpec,
    float_precision: FormatWidthSpec,
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

pub struct FormatIdentRef {
    field_id: FieldId,
    iter_id: IterId,
}

struct TfFormatStreamValueHandle {
    part_idx: usize,
    handled_len: usize,
}
#[derive(Clone, Copy)]
struct OutputState {
    next: usize,
    len: usize,
    run_len: usize,
    contains_raw_bytes: bool,
    error_occured: bool,
}

struct OutputTarget {
    run_len: usize,
    target: Option<NonNull<u8>>,
    target_buffer_offset: Option<NonMaxUsize>,
}

impl Default for OutputState {
    fn default() -> Self {
        Self {
            next: 0,
            len: Default::default(),
            contains_raw_bytes: false,
            error_occured: false,
            run_len: 0,
        }
    }
}

pub struct TfFormat<'a> {
    output_field: FieldId,
    parts: &'a Vec<FormatPart>,
    refs: Vec<FormatIdentRef>,
    input_fields_unique: Vec<FieldId>,
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
    let output_field = sess.record_mgr.add_field(tf_state.match_set_id, None);
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
                        sess.record_mgr
                            .add_field(tf_state.match_set_id, Some(*name))
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
    let mut input_fields_unique: Vec<_> = refs.iter().map(|fir| fir.field_id).collect();
    input_fields_unique.sort_unstable();
    input_fields_unique.dedup();
    let tf = TfFormat {
        output_field,
        parts: &op.parts,
        refs,
        input_fields_unique,
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
) -> Result<(FormatWidthSpec, usize), (usize, Cow<'static, str>)> {
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
            if fmt.get(i).map(|c| c.is_ascii_digit()).unwrap_or(false) {
                let val = unsafe { (&fmt[start..i]).to_str_unchecked() }
                    .parse::<usize>()
                    .map_err(|e| {
                        (
                            start,
                            format!("failed to parse the {context} as an integer: {e}").into(),
                        )
                    })?;
                return Ok((FormatWidthSpec::Value(val), i));
            }
        }
    }
    let mut format_width_ident = SmallString::<[u8; 64]>::new();
    loop {
        if let Some(end) = (&fmt[i..]).find_byteset("${}") {
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
                refs.push(fmt_ref);
                let ref_id = refs.len() - 1;
                return Ok((FormatWidthSpec::Ref(ref_id), i));
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
    key.fill = match next(fmt, i + 1)? {
        '<' => Some((c, FormatFillAlignment::Left)),
        '^' => Some((c, FormatFillAlignment::Center)),
        '>' => Some((c, FormatFillAlignment::Right)),
        _ => None,
    };
    if key.fill.is_some() {
        i += 2;
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
    if c != '}' && c != '.' {
        (key.width, i) = parse_format_width_spec::<false>(fmt, i, refs)?;
        c = next(fmt, i)?;
    }
    if c == '.' {
        (key.float_precision, i) = parse_format_width_spec::<true>(fmt, i + 1, refs)?;
        c = next(fmt, i)?;
    }
    if c != '}' {
        return Err((i, "expected '}' to terminate format key".into()));
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
    func: impl Fn(&mut OutputState),
) {
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
        func(o);
        *output_idx = o.next;
        run_len -= o.run_len;
    }
}
pub fn setup_key_output_state(
    fields: &Universe<FieldId, RefCell<Field>>,
    match_sets: &mut Universe<MatchSetId, MatchSet>,
    fmt: &mut TfFormat,
    k: &FormatKey,
) {
    let ident_ref = &fmt.refs[k.identifier];
    let field = &mut fields[ident_ref.field_id].borrow();
    let mut iter = AutoDerefIter::new(
        fields,
        match_sets,
        ident_ref.field_id,
        field.field_data.get_iter(ident_ref.iter_id),
        None,
    );

    let mut output_index = 0;
    while let Some(range) =
        iter.typed_range_fwd(match_sets, usize::MAX, field_value_flags::BYTES_ARE_UTF8)
    {
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(text) => {
                for (v, rl, _offs) in RefAwareInlineTextIter::from_range(&range, text) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += v.len();
                    });
                }
            }
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += v.len();
                        o.contains_raw_bytes = true;
                    });
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += v.len();
                        o.contains_raw_bytes = true;
                    });
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    let digits = i64_digits(k.add_plus_sign, *v);
                    iter_output_states(fmt, &mut output_index, rl as usize, |o| {
                        o.len += digits;
                    });
                }
            }
            TypedSlice::Unset(_)
            | TypedSlice::Null(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::StreamValueId(_)
            | TypedSlice::Object(_) => {
                iter_output_states(fmt, &mut output_index, range.base.field_count, |o| {
                    o.error_occured = true;
                });
            }
        }
    }
}
unsafe fn write_bytes(
    fmt: &mut TfFormat,
    output_index: &mut usize,
    bytes: &[u8],
    mut run_len: usize,
) {
    while run_len > 0 {
        let o = &mut fmt.output_targets[*output_index];
        if let Some(target) = &mut o.target {
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), target.as_ptr(), bytes.len());
                *target = NonNull::new_unchecked(target.as_ptr().add(bytes.len()));
            }
        }
        run_len -= o.run_len;
        *output_index += 1;
    }
}
fn setup_output_targets(
    op_id: OperatorId,
    fmt: &mut TfFormat,
    output_field: &mut RefMut<Field>,
) -> usize {
    let mut inline_len = 0;
    fmt.output_targets.reserve(fmt.output_states.len());
    let mut output_idx = 0;
    let mut dummy: u8 = 0;
    let dummy_ptr = Some(NonNull::new(&mut dummy as *mut u8).unwrap());
    let mut prev_len = usize::MAX;
    loop {
        let os = &mut fmt.output_states[output_idx];
        let target: Option<NonNull<u8>>;
        let target_buffer_offset;
        if os.error_occured {
            target = None;
            target_buffer_offset = None;
            output_field.field_data.push_error(
                OperatorApplicationError::new("Type Error", op_id),
                os.run_len,
                true,
                false,
            );
            prev_len = usize::MAX;
        } else if os.len < INLINE_STR_MAX_LEN {
            unsafe {
                let fd = output_field.field_data.internals().fd;
                fd.add_header_for_single_value(
                    FieldValueFormat {
                        kind: FieldValueKind::BytesInline,
                        flags: if os.contains_raw_bytes {
                            0
                        } else {
                            field_value_flags::BYTES_ARE_UTF8
                        },
                        size: os.len as FieldValueSize,
                    },
                    os.run_len,
                    os.len == prev_len,
                    false,
                );
                *fd.internals().field_count += os.run_len;
                target_buffer_offset = Some(NonMaxUsize::new_unchecked(os.len));
            }
            target = dummy_ptr;
            inline_len += os.len;
            prev_len = os.len;
        } else {
            let mut buf = Vec::with_capacity(os.len);
            unsafe {
                target = Some(NonNull::new_unchecked(buf.as_mut_ptr()));
                target_buffer_offset = Some(NonMaxUsize::new_unchecked(
                    output_field
                        .field_data
                        .internals()
                        .fd
                        .internals()
                        .data
                        .len(),
                ));
            }
            output_field
                .field_data
                .push_bytes_buffer(buf, os.run_len, true, false);
            prev_len = usize::MAX;
        };
        fmt.output_targets.push(OutputTarget {
            run_len: os.run_len,
            target,
            target_buffer_offset,
        });
        output_idx = os.next;
        if output_idx == 0 {
            break;
        }
    }
    let mut target = unsafe {
        let fdi = output_field.field_data.internals().fd.internals();
        fdi.data.reserve(inline_len);
        fdi.data.as_mut_ptr_range().end
    };
    for tgt in fmt.output_targets.iter_mut() {
        if tgt.target == dummy_ptr {
            unsafe {
                let len = tgt.target_buffer_offset.unwrap_unchecked().get();
                tgt.target = Some(NonNull::new_unchecked(target));
                target = target.add(len);
            }
            tgt.target_buffer_offset = None;
        }
    }
    inline_len
}
fn bump_output_targets_buffer_lengths(fmt: &mut TfFormat, output_field: &mut RefMut<Field>) {
    for t in &fmt.output_targets {
        if let Some(tbo) = t.target_buffer_offset {
            unsafe {
                let buf = &mut *(output_field
                    .field_data
                    .internals()
                    .fd
                    .internals()
                    .data
                    .as_mut_ptr()
                    .add(tbo.get()) as *mut Vec<u8>);
                buf.set_len(
                    t.target
                        .unwrap_unchecked()
                        .as_ptr()
                        .offset_from(buf.as_ptr()) as usize,
                );
            }
        }
    }
}
fn write_fmt_key(
    fields: &Universe<FieldId, RefCell<Field>>,
    match_sets: &mut Universe<MatchSetId, MatchSet>,
    fmt: &mut TfFormat,
    k: &FormatKey,
) {
    let ident_ref = &fmt.refs[k.identifier];
    let field = &mut fields[ident_ref.field_id].borrow();
    let mut iter = AutoDerefIter::new(
        fields,
        match_sets,
        ident_ref.field_id,
        field.field_data.get_iter(ident_ref.iter_id),
        None,
    );

    let mut output_index = 0;
    while let Some(range) = iter.typed_range_fwd(match_sets, usize::MAX, field_value_flags::DEFAULT)
    {
        //TODO: respect format options
        match range.base.data {
            TypedSlice::Reference(_) => unreachable!(),
            TypedSlice::TextInline(_) => unreachable!(),
            TypedSlice::BytesInline(bytes) => {
                for (v, rl, _offs) in RefAwareInlineBytesIter::from_range(&range, bytes) {
                    unsafe { write_bytes(fmt, &mut output_index, v.as_bytes(), rl as usize) };
                }
            }
            TypedSlice::BytesBuffer(bytes) => {
                for (v, rl, _offs) in RefAwareBytesBufferIter::from_range(&range, bytes) {
                    unsafe { write_bytes(fmt, &mut output_index, v.as_bytes(), rl as usize) };
                }
            }
            TypedSlice::Integer(ints) => {
                for (v, rl) in TypedSliceIter::from_range(&range.base, ints) {
                    unsafe {
                        write_bytes(
                            fmt,
                            &mut output_index,
                            i64_to_str(k.add_plus_sign, *v).as_bytes(),
                            rl as usize,
                        )
                    };
                }
            }
            TypedSlice::StreamValueId(_) => todo!(),
            TypedSlice::Unset(_)
            | TypedSlice::Null(_)
            | TypedSlice::Error(_)
            | TypedSlice::Html(_)
            | TypedSlice::Object(_) => {
                let mut rl = range.base.field_count;
                while rl > 0 {
                    let o = &mut fmt.output_states[output_index];
                    rl -= o.run_len;
                    output_index = o.next;
                }
            }
        }
    }
}
pub fn handle_tf_format(sess: &mut JobData<'_>, tf_id: TransformId, fmt: &mut TfFormat) {
    sess.record_mgr.apply_field_commands_for_tf_outputs(
        &sess.tf_mgr.transforms[tf_id],
        std::slice::from_ref(&fmt.output_field),
    );
    let (batch, _input_field_id) = sess.claim_batch(tf_id);
    let op_id = sess.tf_mgr.transforms[tf_id].op_id;
    let mut output_field = sess.record_mgr.fields[fmt.output_field].borrow_mut();
    fmt.output_states.push(OutputState {
        run_len: batch,
        ..Default::default()
    });
    for part in fmt.parts.iter() {
        match part {
            FormatPart::ByteLiteral(v) => fmt.output_states.iter_mut().for_each(|s| {
                s.len += v.len();
                s.contains_raw_bytes = true;
            }),
            FormatPart::TextLiteral(v) => {
                fmt.output_states.iter_mut().for_each(|s| s.len += v.len())
            }
            FormatPart::Key(k) => setup_key_output_state(
                &sess.record_mgr.fields,
                &mut sess.record_mgr.match_sets,
                fmt,
                k,
            ),
        }
    }
    let inline_len = setup_output_targets(op_id, fmt, &mut output_field);
    for part in fmt.parts.iter() {
        match part {
            FormatPart::ByteLiteral(v) => unsafe { write_bytes(fmt, &mut 0, v.as_bytes(), batch) },
            FormatPart::TextLiteral(v) => unsafe { write_bytes(fmt, &mut 0, v.as_bytes(), batch) },
            FormatPart::Key(k) => write_fmt_key(
                &sess.record_mgr.fields,
                &mut sess.record_mgr.match_sets,
                fmt,
                k,
            ),
        }
    }
    unsafe {
        let data = &mut output_field.field_data.internals().fd.internals().data;
        data.set_len(data.len() + inline_len);
    }
    bump_output_targets_buffer_lengths(fmt, &mut output_field);
    fmt.output_states.clear();
    fmt.output_targets.clear();
    sess.tf_mgr.update_ready_state(tf_id);
    sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
}

pub fn handle_tf_format_stream_value_update(
    _sess: &mut JobData<'_>,
    _tf_id: TransformId,
    _tf: &mut TfFormat,
    _svid: StreamValueId,
    _custom: usize,
) {
    todo!();
}

#[cfg(test)]
mod test {
    use bstr::ByteSlice;

    use crate::operations::format::FormatKey;

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
}
