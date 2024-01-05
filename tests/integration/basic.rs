use std::borrow::Cow;

use rstest::rstest;
use scr_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        fork::create_op_fork,
        format::create_op_format,
        join::create_op_join_str,
        key::create_op_key,
        literal::{
            create_op_error, create_op_int, create_op_literal, create_op_str,
            Literal,
        },
        next::create_op_next,
        nop_copy::create_op_nop_copy,
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
        sequence::{create_op_enum, create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::{ChainSetupError, ScrError},
    utils::test_utils::{ErroringStream, SliceReader, TricklingStream},
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("foo", 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn tf_literal() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::String("foo".to_owned()), None))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn counted_tf_literal() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::String("x".to_owned()), Some(3)))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["x", "x", "x"]);
    Ok(())
}

#[test]
fn multi_doc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("foo", 1)
        .push_str("bar", 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}

#[test]
fn trickling_stream() -> Result<(), ScrError> {
    const SIZE: usize = 4096;

    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(3)
        .add_op(create_op_file_reader_custom(
            Box::new(TricklingStream::new("a".as_bytes(), SIZE)),
            0,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    // not using assert_eq here because the output is very large
    assert!(ss.get_data().unwrap().as_slice() == ["a".repeat(SIZE)]);
    Ok(())
}

#[test]
fn sequence() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2"]);
    Ok(())
}

#[test]
fn in_between_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("a", 1)
        .push_str("b", 1)
        .push_str("c", 1)
        .add_op(create_op_regex("[^b]").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a", "c"]);
    Ok(())
}

#[test]
fn drops_surrounding_single_val() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_regex("[1]").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1"]);
    Ok(())
}
#[test]
fn drops_surrounding_range() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 8, 1).unwrap())
        .add_op(create_op_regex("[2-5]").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "4", "5"]);
    Ok(())
}

#[test]
fn basic_key_cow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(1)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_format("{:?}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}

#[test]
fn batched_use_after_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(1)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["1", "2", "3"]);
    Ok(())
}

#[test]
fn double_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::Int(42), None))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("foo: {foo}, bar: {bar}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["foo: 42, bar: 42"]);
    Ok(())
}

#[test]
fn chained_seq() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_int(0, 1)
        .add_op_appending(create_op_seq(1, 6, 1).unwrap())
        .add_op_appending(create_op_seq(6, 11, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        (0..11).map(|v| v.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn unset_field_value() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_format("{foo}{}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        &["x0", "ERROR: in op id 2: unexpected type `undefined` in format key 'foo'"]
    );
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("unexpected type `undefined` in format key 'foo'")
    );
    Ok(())
}

#[test]
fn unset_field_value_debug_repr_is_undefined() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_format("{foo:?}_{}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &["\"x\"_0", "undefined_1"]
    );
    Ok(())
}

#[test]
fn unset_field_value_does_not_trigger_underflow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("x".to_owned()))
        .set_batch_size(1)
        //TODO: investigate why this bug did not trigger for 3 elements
        .add_op(create_op_seq(0, 4, 1).unwrap())
        .add_op(create_op_format("{x:?}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &["\"x\"", "undefined", "undefined", "undefined"]
    );
    Ok(())
}

#[test]
fn seq_enum() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum(0, 5, 1).unwrap())
        .add_op(create_op_format("{foo}{}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["x0", "x1", "x2"]);
    Ok(())
}

#[test]
fn double_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(5)
        .add_op(create_op_seq(0, 15, 1).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_regex("1.*").unwrap())
        .add_op(create_op_format("{a}").unwrap())
        .add_op(create_op_regex(".*1.*").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["1", "10", "11", "12", "13", "14"]
    );
    Ok(())
}

#[test]
fn select() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(5)
        .add_op_with_opts(
            create_op_literal(Literal::String("foo".to_owned()), Some(3)),
            None,
            Some("a"),
            false,
            false,
        )
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("a".to_owned()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "foo", "foo"]);
    Ok(())
}

#[test]
fn select_after_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(5)
        .add_op(create_op_literal(Literal::String("foo".to_owned()), None))
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("a".to_owned()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn basic_cow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("123", 1)
        .add_op(create_op_fork())
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
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(scr_core::options::chain_options::DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap())]
fn cow_not_affecting_original(
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .push_str("123", 1)
        .add_op(create_op_fork())
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
        .add_op(create_op_string_sink(&ss1))
        .add_op(create_op_next())
        .add_op(create_op_string_sink(&ss2))
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["123"]);
    Ok(())
}

#[test]
fn chained_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_batch_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new("bar".as_bytes())),
            0,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["foo", "bar"]);
    Ok(())
}
#[test]
fn tf_literal_yields_to_cont() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_int(1, 3))
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_str("bar", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["foo", "bar", "bar"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
fn tf_file_yields_to_cont(
    #[case] stream_buffer_size: usize,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(stream_buffer_size)
        .add_op(create_op_int(1, 3))
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new(b"foo")),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new(b"bar")),
            0,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar", "bar"]);
    Ok(())
}

#[test]
fn error_on_sbs_0() {
    assert!(matches!(
        ContextBuilder::default()
            .set_stream_buffer_size(0)
            .add_op(create_op_int(1, 3))
            .run()
            .map_err(|e| e.err),
        Err(ScrError::ChainSetupError(ChainSetupError {
            message: Cow::Borrowed("stream buffer size cannot be zero"),
            chain_id: 0
        }))
    ));
}

#[test]
fn stream_error_after_regular_error() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_stream_size_threshold(3)
        .add_op(create_op_error("A", 0))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(ErroringStream::new(5, SliceReader::new(b"BBBBB"))),
            0,
        ))
        .add_op(create_op_join_str("", 1))
        .add_op(create_op_format("{:??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        [
            "(error)\"A\"",
            "~(error)\"ErroringStream: Expected Debug Error\""
        ]
    );
    Ok(())
}

#[test]
fn single_operator() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 10000, 1).unwrap())
        .run()?;
    assert_eq!(ss.get().data.as_slice(), &[] as &[String]);
    Ok(())
}

#[test]
fn seq_with_changing_str_length() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_appending(create_op_seq(1, 11, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(1..11).map(|v| v.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn basic_input_feeder() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("123", 1)
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
        .add_op(create_op_regex("2").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2"]);
    Ok(())
}

#[test]
fn field_refs_in_nopc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("123", 1)
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
        .add_op(create_op_regex("[13]").unwrap())
        .add_op(create_op_nop_copy())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "3"]);
    Ok(())
}
