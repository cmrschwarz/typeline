use std::{borrow::Cow, intrinsics::unreachable};

use bstr::{BStr, BString, ByteSlice, ByteVec};
use smallstr::SmallString;

use crate::{
    fd_ref_iter::{FDAutoDerefIter, FDRefIterLazy},
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
    pub parts: Vec<FormatPart>,
    pub refs_str: Vec<Option<String>>,
    pub refs_idx: Vec<Option<StringStoreEntry>>,
}

pub struct FormatIdentRef {
    field_id: FieldId,
    iter_id: FDIterId,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a Vec<FormatPart>,
    pub refs: Vec<FormatIdentRef>,
    pub input_fields_unique: Vec<FieldId>,
    pub output_lengths: Vec<usize>,
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
        output_lengths: Default::default(),
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

pub fn handle_tf_format(sess: &mut JobData<'_>, tf_id: TransformId, fmt: &mut TfFormat) {
    sess.record_mgr.apply_field_commands_for_tf_outputs(
        &sess.tf_mgr.transforms[tf_id],
        std::slice::from_ref(&fmt.output_field),
    );
    let (batch, _input_field_id) = sess.claim_batch(tf_id);
    let tf = &sess.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id;
    let output_field = sess.record_mgr.fields[fmt.output_field].borrow_mut();
    let starting_pos =
        output_field.field_data.field_count() + output_field.field_data.field_index_offset();
    let mut fd_ref_iter = FDRefIterLazy::default();
    fmt.output_lengths.clear();
    fmt.output_lengths.resize(batch, 0);
    for part in fmt.parts.iter() {
        let mut field_pos = starting_pos;
        match part {
            FormatPart::ByteLiteral(v) => fmt.output_lengths.iter_mut().for_each(|l| *l += v.len()),
            FormatPart::TextLiteral(v) => fmt.output_lengths.iter_mut().for_each(|l| *l += v.len()),
            FormatPart::Key(k) => {
                let ident_ref = &fmt.refs[k.identifier];
                let field = &mut sess.record_mgr.fields[ident_ref.field_id].borrow();
                let mut iter = FDAutoDerefIter::new(
                    &sess.record_mgr.fields,
                    &mut sess.record_mgr.match_sets,
                    field.field_data.get_iter(ident_ref.iter_id),
                    ident_ref.field_id,
                );

                while let Some(range) =
                    iter.typed_range_fwd(&mut sess.record_mgr.match_sets, usize::MAX)
                {
                    match range.data {
                        FDTypedSlice::Reference(refs) => unreachable!(),
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
                    field_pos += range.field_count;
                }
            }
        }
    }
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
