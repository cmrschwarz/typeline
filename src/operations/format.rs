use std::borrow::Cow;

use bstr::{BStr, BString, ByteSlice, ByteVec};
use smallstr::SmallString;

use crate::{
    fd_ref_iter::FDRefIterLazy,
    field_data::{
        fd_iter::{FDIterator, FDTypedSlice, InlineBytesIter, InlineTextIter, TypedSliceIter},
        fd_iter_hall::FDIterId,
        fd_push_interface::FDPushInterface,
        field_value_flags,
    },
    options::argument::CliArgIdx,
    stream_value::StreamValueId,
    utils::string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::{OperatorApplicationError, OperatorCreationError, OperatorSetupError},
    operator::OperatorData,
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
pub enum FormatWidthSpec<IdentifierType> {
    Value(usize),
    Ref(Option<IdentifierType>),
}

impl<T> Default for FormatWidthSpec<T> {
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
pub struct FormatKey<IdentifierType> {
    identifier: Option<IdentifierType>,
    fill: Option<(char, FormatFillAlignment)>,
    add_plus_sign: bool,
    zero_pad_numbers: bool,
    width: FormatWidthSpec<IdentifierType>,
    float_precision: FormatWidthSpec<IdentifierType>,
    alternate_form: bool, // prefix 0x for hex, 0o for octal and 0b for binary, pretty print objects / arrays
    number_format: NumberFormat,
    debug: bool,
    unicode: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FormatPart<IdentifierType> {
    ByteLiteral(BString),
    TextLiteral(String),
    Key(FormatKey<IdentifierType>),
}

type FormatKeyRefId = usize;

pub enum OpFormatParts {
    PreSetup(Vec<FormatPart<String>>),
    Interned(Vec<FormatPart<FormatKeyRefId>>),
}

pub struct OpFormat {
    pub parts: OpFormatParts,
    pub refs: Vec<StringStoreEntry>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub input_field_iter_id: FDIterId,
    pub parts: &'a Vec<FormatPart<FormatKeyRefId>>,
    pub refs: Vec<FieldId>,
}

fn intern_format_width_spec(
    ss: &mut StringStore,
    refs: &mut Vec<StringStoreEntry>,
    fws: FormatWidthSpec<String>,
) -> FormatWidthSpec<FormatKeyRefId> {
    match fws {
        FormatWidthSpec::Value(v) => FormatWidthSpec::Value(v),
        FormatWidthSpec::Ref(id) => FormatWidthSpec::Ref(id.map(|id| {
            refs.push(ss.intern_moved(id));
            refs.len() - 1
        })),
    }
}

fn intern_format_part(
    ss: &mut StringStore,
    refs: &mut Vec<StringStoreEntry>,
    fp: FormatPart<String>,
) -> FormatPart<FormatKeyRefId> {
    match fp {
        FormatPart::ByteLiteral(v) => FormatPart::ByteLiteral(v),
        FormatPart::TextLiteral(v) => FormatPart::TextLiteral(v),
        FormatPart::Key(k) => FormatPart::Key(FormatKey {
            identifier: k.identifier.map(|id| {
                refs.push(ss.intern_moved(id));
                refs.len() - 1
            }),
            fill: k.fill,
            add_plus_sign: k.add_plus_sign,
            zero_pad_numbers: k.zero_pad_numbers,
            width: intern_format_width_spec(ss, refs, k.width),
            float_precision: intern_format_width_spec(ss, refs, k.float_precision),
            alternate_form: k.alternate_form,
            number_format: k.number_format,
            debug: k.debug,
            unicode: k.unicode,
        }),
    }
}

pub fn setup_op_format(
    string_store: &mut StringStore,
    op: &mut OpFormat,
) -> Result<(), OperatorSetupError> {
    let parts = std::mem::replace(&mut op.parts, OpFormatParts::Interned(Vec::default()));
    if let OpFormatParts::PreSetup(parts) = parts {
        op.parts = OpFormatParts::Interned(
            parts
                .into_iter()
                .map(|p| intern_format_part(string_store, &mut op.refs, p))
                .collect(),
        )
    } else {
        unreachable!();
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
    let parts = if let OpFormatParts::Interned(ref parts) = op.parts {
        parts
    } else {
        panic!("attempted to create a TfFormat without calling setup_op_format first")
    };
    let input_field_iter_id = sess.record_mgr.fields[tf_state.input_field]
        .borrow_mut()
        .field_data
        .claim_iter();
    let tf = TfFormat {
        output_field,
        input_field_iter_id,
        parts,
        refs: op
            .refs
            .iter()
            .map(|r| {
                let id = sess.record_mgr.match_sets[tf_state.match_set_id]
                    .field_name_map
                    .get(r)
                    .and_then(|fields| fields.back().cloned())
                    .unwrap_or_else(|| sess.record_mgr.add_field(tf_state.match_set_id, Some(*r)));
                sess.record_mgr.fields[id]
                    .borrow_mut()
                    .added_as_placeholder_by_tf = Some(tf_id);
                id
            })
            .collect(),
    };
    (TransformData::Format(tf), output_field)
}

fn create_format_literal(fmt: BString) -> FormatPart<String> {
    match String::try_from(fmt) {
        Ok(v) => FormatPart::TextLiteral(v),
        Err(err) => FormatPart::ByteLiteral(BString::from(err.into_vec())),
    }
}

const NO_CLOSING_BRACE_ERR: Cow<'static, str> = Cow::Borrowed("format key has no closing '}'");

pub fn parse_format_width_spec<const FOR_FLOAT_PREC: bool>(
    fmt: &BStr,
    start: usize,
) -> Result<(FormatWidthSpec<String>, usize), (usize, Cow<'static, str>)> {
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
                return Ok((
                    FormatWidthSpec::Ref(Some(format_width_ident.into_string())),
                    i,
                ));
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
    key: &mut FormatKey<String>,
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
        (key.width, i) = parse_format_width_spec::<false>(fmt, i)?;
        c = next(fmt, i)?;
    }
    if c == '.' {
        (key.float_precision, i) = parse_format_width_spec::<true>(fmt, i + 1)?;
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
) -> Result<(FormatKey<String>, usize), (usize, Cow<'static, str>)> {
    debug_assert!(fmt[start] == '{' as u8);
    let mut i = start + 1;
    let mut key = FormatKey::default();
    if let Some(mut end) = (&fmt[i..]).find_byteset("}:") {
        end += i;
        let c0 = fmt[end] as char;
        if end > i {
            key.identifier = Some(
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
            );
        }
        i = end;
        if c0 == ':' {
            i = parse_format_flags(fmt, i, &mut key)?;
        }
        return Ok((key, i + 1));
    }
    return Err((fmt.len(), NO_CLOSING_BRACE_ERR));
}

pub fn parse_format_string(
    fmt: &BStr,
) -> Result<Vec<FormatPart<String>>, (usize, Cow<'static, str>)> {
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
            let (key, end) = parse_format_key(fmt, i)?;
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
    Ok(OperatorData::Format(OpFormat {
        parts: OpFormatParts::PreSetup(parse_format_string(val).map_err(|(i, msg)| {
            OperatorCreationError {
                message: format!("format string index {}: {}", i, msg).into(),
                cli_arg_idx: arg_idx,
            }
        })?),
        refs: Vec::default(),
    }))
}
pub fn create_op_format(val: &BStr) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val), None)
}
pub fn create_op_format_from_str(val: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_format(Some(val.as_bytes().as_bstr()), None)
}

pub fn handle_tf_format(sess: &mut JobData<'_>, tf_id: TransformId, fmt: &mut TfFormat) {
    sess.record_mgr.apply_field_commands_for_tf_outputs(
        &sess.tf_mgr.transforms[tf_id],
        std::slice::from_ref(&fmt.output_field),
    );
    let (batch, input_field_id) = sess.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id;
    let input_field = sess.record_mgr.fields[input_field_id].borrow_mut();
    let mut fd_ref_iter = FDRefIterLazy::default();
    let mut iter = input_field
        .field_data
        .get_iter(fmt.input_field_iter_id)
        .bounded(0, batch);
    let mut field_idx = iter.get_next_field_pos();

    while let Some(range) = iter.typed_range_fwd(usize::MAX, field_value_flags::BYTES_ARE_UTF8) {
        match range.data {
            FDTypedSlice::TextInline(text) => {
                for (_v, _rl) in InlineTextIter::from_typed_range(&range, text) {
                    todo!();
                }
            }
            FDTypedSlice::BytesInline(bytes) => {
                for (_v, _rl) in InlineBytesIter::from_typed_range(&range, bytes) {
                    todo!();
                }
            }
            FDTypedSlice::BytesBuffer(bytes) => {
                for (_v, _rl) in TypedSliceIter::from_typed_range(&range, bytes) {
                    todo!();
                }
            }
            FDTypedSlice::Reference(refs) => {
                let mut iter = fd_ref_iter.setup_iter_from_typed_range(
                    &sess.record_mgr.fields,
                    &mut sess.record_mgr.match_sets,
                    field_idx,
                    &range,
                    refs,
                );
                while let Some(_fr) =
                    iter.typed_range_fwd(&mut sess.record_mgr.match_sets, usize::MAX)
                {
                    todo!();
                }
            }
            FDTypedSlice::Unset(_)
            | FDTypedSlice::Null(_)
            | FDTypedSlice::Integer(_)
            | FDTypedSlice::Error(_)
            | FDTypedSlice::Html(_)
            | FDTypedSlice::StreamValueId(_)
            | FDTypedSlice::Object(_) => {
                sess.record_mgr.fields[fmt.output_field]
                    .borrow_mut()
                    .field_data
                    .push_error(
                        OperatorApplicationError::new("format type error", op_id),
                        range.field_count,
                        true,
                        true,
                    );
            }
        }
        field_idx += range.field_count;
    }
    input_field
        .field_data
        .store_iter(fmt.input_field_iter_id, iter);
    drop(input_field);
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
        assert_eq!(parse_format_string(&[].as_bstr()).unwrap(), &[]);
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
            assert_eq!(
                parse_format_string(lit.as_bytes().as_bstr()).unwrap(),
                &[FormatPart::TextLiteral(res.to_owned())]
            );
        }
    }

    #[test]
    fn two_keys() {
        let mut a = FormatKey::default();
        a.identifier = Some("a".to_owned());
        let mut b = FormatKey::default();
        b.identifier = Some("b".to_owned());
        assert_eq!(
            parse_format_string("foo{{{a}}}__{b}".as_bytes().as_bstr()).unwrap(),
            &[
                FormatPart::TextLiteral("foo{".to_owned()),
                FormatPart::Key(a),
                FormatPart::TextLiteral("}__".to_owned()),
                FormatPart::Key(b),
            ]
        );
    }
}
