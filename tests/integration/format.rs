use indexland::IndexingType;
use rstest::rstest;
use typeline::operators::{
    operator::OperatorId,
    string_sink::{create_op_string_sink, StringSinkHandle},
};
use typeline_core::{
    operators::{
        errors::OperatorApplicationError,
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        literal::{
            create_op_bytes, create_op_error, create_op_null, create_op_str,
            create_op_stream_error, create_op_stream_str, create_op_v,
        },
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
    utils::{int_string_conversions::i64_to_str, test_utils::SliceReader},
};
use typeline_ext_utils::sequence::{create_op_enum, create_op_seq};

#[test]
fn debug_format_surrounds_with_quotes() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op_aggregate_appending([
            create_op_bytes(b"bar"),
            create_op_error("baz"),
        ])
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"foo\"", "b\"bar\"", "(error)\"baz\""]);
    Ok(())
}

#[test]
fn more_debug_format_surrounds_with_quotes() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op_aggregate_appending([
            create_op_bytes(b"bar"),
            create_op_error("baz"),
        ])
        .add_op(create_op_format("{:??}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"foo\"", "b\"bar\"", "(error)\"baz\""]);
    Ok(())
}

#[rstest]
#[case("{:?}", "(error)\"A\"")]
#[case("{:??}", "(error)\"A\"")]
fn error_formatting(
    #[case] fmt_string: &str,
    #[case] result: &str,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_error("A"))
        .add_op(create_op_format(fmt_string).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, [result]);
    Ok(())
}

#[rstest]
#[case("{:?}", "(error)\"A\"")]
#[case("{:??}", "~(error)\"A\"")]
fn stream_error_formatting(
    #[case] fmt_string: &str,
    #[case] result: &str,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_stream_error("A"))
        .add_op(create_op_format(fmt_string).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, [result]);
    Ok(())
}

#[test]
fn format_width_spec() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 6, 1).unwrap())
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("{foo:~^bar$}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["x", "x", "~x", "~x~", "~~x~", "~~x~~"]);
    Ok(())
}

#[test]
fn format_width_spec_over_stream() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(1)?
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "abc".as_bytes(),
            }),
            6,
        ))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum(2, 8, 1).unwrap())
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("{foo:~^bar$}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["abc", "abc", "~abc", "~abc~", "~~abc~", "~~abc~~"]);
    Ok(())
}

#[test]
fn nonexisting_key() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_str("x", 3)
        .add_op(create_op_format("{foo}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("unexpected type `undefined` in format key 'foo'") /* TODO: better error message */
    );
    Ok(())
}
#[test]
fn nonexisting_format_width_key() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_str("x", 3)
        .add_op(create_op_format("{:foo$.bar$}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().get_first_error_message(),
        Some(
            "unexpected type `undefined` in width spec 'foo' of format key #1"
        )
    );
    Ok(())
}

#[test]
fn format_after_surrounding_drop() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 10, 1).unwrap())
        .add_op(create_op_regex("[3-5]").unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["3", "4", "5"]);
    Ok(())
}

#[test]
fn batched_format_after_drop() -> Result<(), TypelineError> {
    const COUNT: i64 = 20;
    let res = ContextBuilder::without_exts()
        .set_batch_size(3)
        .unwrap()
        .add_op(create_op_seq(0, COUNT, 1).unwrap())
        .add_op(create_op_regex(".*[3].*").unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(
        res,
        (0..COUNT)
            .filter_map(|v| {
                let v = i64_to_str(false, v).to_string();
                if v.contains('3') {
                    Some(v)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn stream_into_format() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(1)?
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "xyz".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("a -> {} -> b").unwrap())
        .run_collect_stringified()?;
    assert_eq!(&res, &["a -> xyz -> b"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn stream_into_multiple_different_formats(
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .push_str("foo", 1)
        .push_str("bar", 1)
        .add_op(create_op_key("key".to_owned()))
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "xxx".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("{key}: {}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["foo: xxx", "bar: xxx"]);
    Ok(())
}

#[test]
fn dup_between_format_and_key() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .push_str("xxx", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(
            create_op_regex_with_opts(
                ".",
                RegexOptions {
                    multimatch: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .add_op(create_op_format("{foo}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["xxx", "xxx", "xxx"]);
    Ok(())
}

#[test]
fn debug_string_escapes() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .push_str("\n", 1)
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &[r#""\n""#]);
    Ok(())
}

#[test]
fn debug_bytes_escapes() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .push_bytes(b"\n\x00", 1)
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &[r#"b"\n\x00""#]);
    Ok(())
}

#[test]
fn debug_bytes_escapes_in_stream() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .set_stream_buffer_size(1)?
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "\n".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &[r#"b"\n""#]);
    Ok(())
}

#[test]
fn sandwiched_format() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_format("{:?}").unwrap())
        .add_op(
            create_op_regex_with_opts(
                ".",
                RegexOptions {
                    multimatch: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .run_collect_stringified()?;
    let q = "\"";
    assert_eq!(res, &[q, "0", q, q, "1", q, q, "2", q]);
    Ok(())
}

#[test]
fn binary_string_formatting() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_v("b'\\xFF'").unwrap())
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &[r#"b"\xFF""#]);
    Ok(())
}

#[test]
fn debug_format_stream_value() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_stream_str("foo"))
        .add_op(create_op_format("{:??}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &[r#"~"foo""#]);
    Ok(())
}

#[test]
fn null_format_error() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_null())
        .add_op(create_op_format("{}").unwrap())
        .run_collect_stringified();
    let Err(res) = res else { panic!() };
    assert_eq!(
        res.err,
        TypelineError::OperationApplicationError(
            OperatorApplicationError::new(
                "unexpected type `null` in format key #1",
                OperatorId::ONE,
            )
        )
    );
    Ok(())
}
