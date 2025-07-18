use rstest::rstest;
use typeline::{
    operators::{
        foreach::create_op_foreach,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    utils::maybe_text::MaybeText,
};
use typeline_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        literal::{
            create_op_error, create_op_int, create_op_literal, create_op_str,
            create_op_str_n, Literal,
        },
        regex::{create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
    utils::test_utils::{ErroringStream, SliceReader},
};
use typeline_ext_utils::{
    dup::create_op_dup,
    join::{create_op_join, create_op_join_str},
    sequence::{create_op_enum, create_op_seq, create_op_seqn},
};

#[test]
fn join() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(1, 4, 1).unwrap())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            None,
            false,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["1,2,3"]);
    Ok(())
}

#[test]
fn join_groups() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_seqn(1, 3, 1).unwrap(),
            create_op_join(None, None, false),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["123", "123", "123"]);
    Ok(())
}

#[test]
fn join_size_one_groups() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_foreach([create_op_join(None, None, false)]))
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3"]);
    Ok(())
}

#[test]
fn join_bounded_groups() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 2, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_seqn(1, 3, 1).unwrap(),
            create_op_join(Some(MaybeText::from_bytes(b",")), Some(2), false),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["1,2", "3", "1,2", "3"]);
    Ok(())
}

#[test]
fn join_single() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(1, 2, 1).unwrap())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            Some(2),
            false,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["1"]);
    Ok(())
}

#[test]
fn join_drop_incomplete() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_join(None, Some(2), true))
        .run_collect_stringified()?;
    assert_eq!(res, ["12"]);
    Ok(())
}
#[test]
fn join_empty() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_dup(0))
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, [""]);
    Ok(())
}

#[test]
fn join_no_sep() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["12345"]);
    Ok(())
}

#[test]
fn join_streams() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .add_op_aggregate([
            create_op_file_reader_custom(
                Box::new(SliceReader::new("abc".as_bytes())),
                0,
            ),
            create_op_file_reader_custom(
                Box::new(SliceReader::new("def".as_bytes())),
                0,
            ),
        ])
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            None,
            false,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["abc, def"]);
    Ok(())
}

#[test]
fn join_after_append() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_aggregate([create_op_str("foo"), create_op_str("bar")])
        .add_op(create_op_join_str(", ", 0))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo, bar"]);
    Ok(())
}

#[test]
fn join_after_enum() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str_n("foo", 2))
        .add_op(create_op_enum(0, i64::MAX, 1).unwrap())
        .add_op(create_op_join_str(",", 0))
        .run_collect_stringified()?;
    assert_eq!(res, ["0,1"]);
    Ok(())
}

#[test]
fn join_seq_into_stream() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_size_threshold(2)?
        .set_batch_size(2)?
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_join_str(",", 0))
        .add_op(create_op_format("{:#??}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["~\"1,2,3,4,5\""]);
    Ok(())
}

#[test]
fn join_dropped_streams() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .add_op_aggregate([
            create_op_file_reader_custom(
                Box::new(SliceReader::new("foo".as_bytes())),
                0,
            ),
            create_op_literal(Literal::Int(1)),
            create_op_file_reader_custom(
                Box::new(SliceReader::new("bar".as_bytes())),
                0,
            ),
        ])
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            Some(2),
            true,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo, 1"]);
    Ok(())
}

#[test]
fn stream_error_in_join() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default(); // we want to observe the error
    ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .add_op_aggregate([
            create_op_file_reader_custom(
                Box::new(SliceReader::new("foo".as_bytes())),
                0,
            ),
            create_op_file_reader_custom(
                Box::new(ErroringStream::new(
                    2,
                    SliceReader::new("bar".as_bytes()),
                )),
                0,
            ),
            create_op_literal(Literal::Int(1)),
        ])
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b", ")),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        // TODO: the aggregator transform taking an op id makes
        // this error message even worse than it previously was
        // figure out a better way to do this!
        ["ERROR: in op id 2: ErroringStream: Error"]
    );
    Ok(())
}

#[test]
fn stream_into_dup_into_join_with_regex() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .set_batch_size(2)?
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
        .run_collect_stringified()?;
    assert_eq!(res, ["foo,foo,foo"]);
    Ok(())
}

#[test]
fn stream_into_dup_into_join() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .set_batch_size(2)?
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
        .run_collect_stringified()?;
    assert_eq!(res, ["foo,foo,foo"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_turns_into_stream(
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)?
        .set_stream_size_threshold(2)?
        .add_op_aggregate([create_op_str("foo"), create_op_str("bar")])
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format("{:#??}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["~\"foo,bar\""]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_on_error(#[case] batch_size: usize) -> Result<(), TypelineError> {
    // TODO: used to stringify as '~(error)..', no longer does because `+` is
    // 'lazy' fix that or add a second test
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_batch_size(batch_size)?
        .set_stream_size_threshold(2)?
        .add_op_aggregate([create_op_str("foo"), create_op_error("bar")])
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format("{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["ERROR: in op id 2: bar"]);

    Ok(())
}

#[test]
fn join_as_stream_producer() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_size_threshold(1)?
        .add_op_aggregate([
            create_op_str("AAA"),
            create_op_int(42),
            create_op_str("BBB"),
        ])
        .add_op(create_op_join_str(", ", 0))
        .run_collect_stringified()?;
    assert_eq!(res, ["AAA, 42, BBB"]);
    Ok(())
}

#[test]
fn join_with_value_between_streams() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_size_threshold(2)?
        .set_stream_buffer_size(2)?
        .add_op_aggregate([
            create_op_file_reader_custom(
                Box::new(SliceReader::new(b"AAA")),
                0,
            ),
            create_op_int(42),
            create_op_file_reader_custom(
                Box::new(SliceReader::new(b"BBB")),
                0,
            ),
        ])
        .add_op(create_op_join_str(", ", 0))
        .run_collect_stringified()?;
    assert_eq!(res, ["AAA, 42, BBB"]);
    Ok(())
}
