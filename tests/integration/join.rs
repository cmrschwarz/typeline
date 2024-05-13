use rstest::rstest;
use scr::{
    operators::foreach::create_op_foreach, utils::maybe_text::MaybeText,
};
use scr_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        join::{create_op_join, create_op_join_str},
        key::create_op_key,
        literal::{
            create_op_error, create_op_int, create_op_literal, create_op_str,
            create_op_str_n, Literal,
        },
        regex::{create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
        sequence::{create_op_enum, create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::{ErroringStream, SliceReader},
};
use scr_ext_utils::dup::create_op_dup;

#[test]
fn join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(1, 4, 1).unwrap())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            None,
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1,2,3"]);
    Ok(())
}

#[test]
fn join_groups() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach())
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["123", "123", "123"]);
    Ok(())
}

#[test]
fn join_size_one_groups() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach())
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3"]);
    Ok(())
}

#[test]
fn join_bounded_groups() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_seqn(1, 2, 1).unwrap())
        .add_op(create_op_foreach())
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            Some(2),
            false,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["1,2", "3", "1,2", "3"]);
    Ok(())
}

#[test]
fn join_single() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(1, 2, 1).unwrap())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            Some(2),
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1"]);
    Ok(())
}

#[test]
fn join_drop_incomplete() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_join(None, Some(2), true))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["12"]);
    Ok(())
}
#[test]
fn join_empty() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_dup(0))
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), [""]);
    Ok(())
}

#[test]
fn join_no_sep() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["12345"]);
    Ok(())
}

#[test]
fn join_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("abc".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new("def".as_bytes())),
            0,
        ))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            None,
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["abc, def"]);
    Ok(())
}

#[test]
fn join_after_append() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo"))
        .add_op_appending(create_op_str("bar"))
        .add_op(create_op_join_str(", ", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo, bar"]);
    Ok(())
}

#[test]
fn join_after_enum() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str_n("foo", 2))
        .add_op(create_op_enum(0, i64::MAX, 1).unwrap())
        .add_op(create_op_join_str(",", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0,1"]);
    Ok(())
}

#[test]
fn join_seq_into_stream() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .set_stream_size_threshold(2)
        .set_batch_size(2)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_join_str(",", 0))
        .add_op(create_op_format("{:#??}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["~\"1,2,3,4,5\""]);
    Ok(())
}

#[test]
fn join_dropped_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_literal(Literal::Int(1)))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new("bar".as_bytes())),
            0,
        ))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            Some(2),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo, 1"]);
    Ok(())
}

#[test]
fn stream_error_in_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(ErroringStream::new(
                2,
                SliceReader::new("bar".as_bytes()),
            )),
            0,
        ))
        .add_op_appending(create_op_literal(Literal::Int(1)))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        ["ERROR: in op id 1: ErroringStream: Error"]
    );
    Ok(())
}

#[test]
fn stream_into_dup_into_join_with_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_batch_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_str("123"))
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
        .add_op(create_op_select("foo".to_owned()))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["foo,foo,foo"]);
    Ok(())
}

#[test]
fn stream_into_dup_into_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_batch_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op(create_op_dup(3))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["foo,foo,foo"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_turns_into_stream(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .set_stream_size_threshold(2)
        .add_op(create_op_str("foo"))
        .add_op_appending(create_op_str("bar"))
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format("{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["~\"foo,bar\""]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_on_error(#[case] batch_size: usize) -> Result<(), ScrError> {
    // TODO: used to stringify as '~(error)..', no longer does because `+` is
    // 'lazy' fix that or add a second test
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .set_stream_size_threshold(2)
        .add_op(create_op_str("foo"))
        .add_op_appending(create_op_error("bar"))
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format("{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["ERROR: in op id 1: bar"]);

    Ok(())
}

#[test]
fn join_as_stream_producer() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_size_threshold(1)
        .add_op(create_op_str("AAA"))
        .add_op_appending(create_op_int(42))
        .add_op_appending(create_op_str("BBB"))
        .add_op(create_op_join_str(", ", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["AAA, 42, BBB"]);
    Ok(())
}

#[test]
fn join_with_value_between_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_stream_size_threshold(2)
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new(b"AAA")),
            0,
        ))
        .add_op_appending(create_op_int(42))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new(b"BBB")),
            0,
        ))
        .add_op(create_op_join_str(", ", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["AAA, 42, BBB"]);
    Ok(())
}
