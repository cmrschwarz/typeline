use rstest::rstest;
use scr_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        literal::{
            create_op_bytes, create_op_error, create_op_str,
            create_op_stream_error,
        },
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::{int_string_conversions::i64_to_str, test_utils::SliceReader},
};

#[test]
fn debug_format_surrounds_with_quotes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_bytes(b"bar", 0))
        .add_op_appending(create_op_error("baz", 0))
        .add_op(create_op_format(b"{:?}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["\"foo\"", "'bar'", "(error)\"baz\""]
    );
    Ok(())
}

#[test]
fn more_debug_format_surrounds_with_quotes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_bytes(b"bar", 0))
        .add_op_appending(create_op_error("baz", 0))
        .add_op(create_op_format(b"{:??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        ["\"foo\"", "'bar'", "(error)\"baz\""]
    );
    Ok(())
}

#[rstest]
#[case("{:?}", "(error)\"A\"")]
#[case("{:??}", "(error)\"A\"")]
fn error_formatting(
    #[case] fmt_string: &str,
    #[case] result: &str,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_error("A", 1))
        .add_op(create_op_format(fmt_string.as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), [result]);
    Ok(())
}

#[rstest]
#[case("{:?}", "(error)\"A\"")]
#[case("{:??}", "~(error)\"A\"")]
fn stream_error_formatting(
    #[case] fmt_string: &str,
    #[case] result: &str,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_stream_error("A", 1))
        .add_op(create_op_format(fmt_string.as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), [result]);
    Ok(())
}

#[test]
fn format_width_spec() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 6)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 6, 1).unwrap())
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("{foo:~^bar$}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &["x", "x", "~x", "~x~", "~~x~", "~~x~~"]
    );
    Ok(())
}

#[test]
fn format_width_spec_over_stream() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "abc".as_bytes(),
            }),
            6,
        ))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(2, 8, 1).unwrap())
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("{foo:~^bar$}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &["abc", "abc", "~abc", "~abc~", "~~abc~", "~~abc~~"]
    );
    Ok(())
}

#[test]
fn nonexisting_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{foo}".as_bytes()).unwrap())
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
fn nonexisting_format_width_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{:foo$}".as_bytes()).unwrap())
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
fn format_after_surrounding_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 10, 1).unwrap())
        .add_op(create_op_regex("[3-5]").unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["3", "4", "5"]);
    Ok(())
}

#[test]
fn batched_format_after_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    const COUNT: i64 = 20;
    ContextBuilder::default()
        .set_batch_size(3)
        .add_op(create_op_seq(0, COUNT, 1).unwrap())
        .add_op(create_op_regex(".*[3].*").unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..COUNT)
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
fn stream_into_format() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "abc".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("a -> {} -> b".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a -> abc -> b"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn stream_into_multiple_different_formats(
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .push_str("foo", 1)
        .push_str("bar", 1)
        .add_op(create_op_key("key".to_owned()))
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "xxx".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("{key}: {}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo: xxx", "bar: xxx"]);
    Ok(())
}

#[test]
fn dup_between_format_and_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
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
        .add_op(create_op_format("{foo}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["xxx", "xxx", "xxx"]);
    Ok(())
}

#[test]
fn debug_string_escapes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .push_str("\n", 1)
        .add_op(create_op_format("{:?}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &[r#""\n""#]);
    Ok(())
}

#[test]
fn debug_bytes_escapes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .push_bytes(b"\n\x00", 1)
        .add_op(create_op_format("{:?}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &[r#"'\n\x00'"#]);
    Ok(())
}

#[test]
fn debug_bytes_escapes_in_stream() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "\n".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("{:?}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &[r#"'\n'"#]);
    Ok(())
}

#[test]
fn sandwiched_format() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_format("{:?}".as_bytes()).unwrap())
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
        .add_op(create_op_string_sink(&ss))
        .run()?;
    let q = "\"";
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &[q, "0", q, q, "1", q, q, "2", q]
    );
    Ok(())
}
