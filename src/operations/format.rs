use std::borrow::Cow;

use bstr::{BStr, BString, ByteSlice, ByteVec};
use smallstr::SmallString;

use crate::{
    utils::string_store::{StringStore, StringStoreEntry},
    worker_thread_session::{FieldId, JobData},
};

use super::transform::{TransformData, TransformState};

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
    Ref(Option<StringStoreEntry>),
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
    identifier: Option<StringStoreEntry>,
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

pub struct OpFormat {
    pub parts: Vec<FormatPart>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a [FormatPart],
}

pub fn setup_tf_format<'a>(
    sess: &mut JobData,
    op: &'a OpFormat,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    //TODO: cache field indices...
    let output_field = sess.record_mgr.add_field(tf_state.match_set_id, None);
    let tf = TfFormat {
        output_field,
        parts: &op.parts,
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
    ss: &mut StringStore,
    fmt: &BStr,
    start: usize,
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
                return Ok((
                    FormatWidthSpec::Ref(Some(ss.intern_moved(format_width_ident.into_string()))),
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
    ss: &mut StringStore,
    fmt: &BStr,
    start: usize,
    key: &mut FormatKey,
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
        (key.width, i) = parse_format_width_spec::<false>(ss, fmt, i)?;
        c = next(fmt, i)?;
    }
    if c == '.' {
        (key.float_precision, i) = parse_format_width_spec::<true>(ss, fmt, i + 1)?;
        c = next(fmt, i)?;
    }
    if c != '}' {
        return Err((i, "expected '}' to terminate format key".into()));
    }
    Ok(i)
}
pub fn parse_format_key(
    ss: &mut StringStore,
    fmt: &BStr,
    start: usize,
) -> Result<(FormatKey, usize), (usize, Cow<'static, str>)> {
    debug_assert!(fmt[start] == '{' as u8);
    let mut i = start + 1;
    let mut key = FormatKey::default();
    if let Some(mut end) = (&fmt[i..]).find_byteset("}:") {
        end += i;
        let c0 = fmt[end] as char;
        if end > i {
            key.identifier = Some(ss.intern_cloned((&fmt[i..end]).to_str().map_err(|e| {
                (
                    i + e.valid_up_to(),
                    "the identifier for the format key identifier must be valid utf-8".into(),
                )
            })?));
        }
        i = end;
        if c0 == ':' {
            i = parse_format_flags(ss, fmt, i, &mut key)?;
        }
        return Ok((key, i + 1));
    }
    return Err((fmt.len(), NO_CLOSING_BRACE_ERR));
}

pub fn parse_format_string(
    ss: &mut StringStore,
    fmt: &BStr,
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
            let (key, end) = parse_format_key(ss, fmt, i)?;
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

#[cfg(test)]
mod test {
    use bstr::ByteSlice;

    use crate::{operations::format::FormatKey, utils::string_store::StringStore};

    use super::{parse_format_string, FormatPart};

    #[test]
    fn empty_format_string() {
        assert_eq!(
            parse_format_string(&mut StringStore::default(), &[].as_bstr()).unwrap(),
            &[]
        );
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
                parse_format_string(&mut StringStore::default(), lit.as_bytes().as_bstr()).unwrap(),
                &[FormatPart::TextLiteral(res.to_owned())]
            );
        }
    }

    #[test]
    fn two_keys() {
        let mut ss = StringStore::default();
        let mut a = FormatKey::default();
        a.identifier = Some(ss.intern_cloned("a"));
        let mut b = FormatKey::default();
        b.identifier = Some(ss.intern_cloned("b"));
        assert_eq!(
            parse_format_string(&mut ss, "foo{{{a}}}__{b}".as_bytes().as_bstr()).unwrap(),
            &[
                FormatPart::TextLiteral("foo{".to_owned()),
                FormatPart::Key(a),
                FormatPart::TextLiteral("}__".to_owned()),
                FormatPart::Key(b),
            ]
        );
    }
}
