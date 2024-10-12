use rstest::rstest;
use scr::{
    operators::{
        aggregator::{create_op_aggregate, create_op_aggregate_appending},
        foreach::create_op_foreach,
        print::{create_op_print_with_opts, PrintOptions},
        sequence::create_op_enum_unbounded,
        utils::writable::MutexedWriteableTargetOwner,
    },
    options::chain_settings::SettingConversionError,
    utils::test_utils::int_sequence_strings,
};
use scr_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        fork::create_op_fork,
        format::create_op_format,
        join::create_op_join_str,
        key::create_op_key,
        literal::{
            create_op_error, create_op_int_n, create_op_literal,
            create_op_literal_n, create_op_str, Literal,
        },
        nop_copy::create_op_nop_copy,
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
        sequence::{create_op_enum, create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::{
        chain_settings::{ChainSetting, SettingBatchSize},
        context_builder::ContextBuilder,
    },
    scr_error::ScrError,
    utils::test_utils::{ErroringStream, SliceReader, TricklingStream},
};
use scr_ext_utils::{
    dup::create_op_dup, string_utils::create_op_chars, sum::create_op_sum,
    tail::create_op_tail,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("foo", 1)
        .run_collect_stringified()?;
    assert_eq!(res, ["foo"]);
    Ok(())
}

#[test]
fn tf_literal() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_literal(Literal::Text("foo".to_owned())))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo"]);
    Ok(())
}

#[test]
fn counted_tf_literal() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_literal_n(Literal::Text("x".to_owned()), 3))
        .run_collect_stringified()?;
    assert_eq!(res, ["x", "x", "x"]);
    Ok(())
}

#[test]
fn multi_doc() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("foo", 1)
        .push_str("bar", 1)
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar"]);
    Ok(())
}

#[test]
fn trickling_stream() -> Result<(), ScrError> {
    const SIZE: usize = 4096;

    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(3)?
        .add_op(create_op_file_reader_custom(
            Box::new(TricklingStream::new("a".as_bytes(), SIZE)),
            0,
        ))
        .run_collect_stringified()?;
    // not using assert_eq here because the output is very large
    assert!(res == ["a".repeat(SIZE)]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
fn sequence(#[case] batch_size: usize) -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1", "2"]);
    Ok(())
}

#[rstest]
#[case(1, 1)]
#[case(2, 1)]
#[case(3, 1)]
#[case(1, 2)]
#[case(2, 2)]
#[case(3, 2)]
fn double_sequence(
    #[case] batch_size_1: usize,
    #[case] batch_size_2: usize,
) -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size_1)
        .unwrap()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .set_batch_size(batch_size_2)
        .unwrap()
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1"].repeat(3));
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(100)]
fn triple_sequence(#[case] batch_size: usize) -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op_with_key("a", create_op_seq(0, 2, 1).unwrap())
        .add_op_with_key("b", create_op_seq(0, 2, 1).unwrap())
        .add_op_with_key("c", create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_format("{a}{b}{c}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, (0..=7).map(|v| format!("{v:03b}")).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn in_between_drop() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("a", 1)
        .push_str("b", 1)
        .push_str("c", 1)
        .add_op(create_op_regex("[^b]").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "c"]);
    Ok(())
}

#[test]
fn drops_surrounding_single_val() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_regex("[1]").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1"]);
    Ok(())
}
#[test]
fn drops_surrounding_range() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 8, 1).unwrap())
        .add_op(create_op_regex("[2-5]").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "3", "4", "5"]);
    Ok(())
}

#[test]
fn basic_key_cow() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(1)
        .unwrap()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_format("{:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3"]);
    Ok(())
}

#[test]
fn batched_use_after_key() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(1)
        .unwrap()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_key("foo".to_owned()))
        .run_collect_stringified()?;
    assert_eq!(res, &["1", "2", "3"]);
    Ok(())
}

#[test]
fn double_key() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_literal(Literal::Int(42)))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("foo: {foo}, bar: {bar}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["foo: 42, bar: 42"]);
    Ok(())
}

#[test]
fn chained_seq() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_aggregate([
            create_op_seq(0, 6, 1).unwrap(),
            create_op_seq(6, 11, 1).unwrap(),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, (0..11).map(|v| v.to_string()).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn chained_seq_with_input_data() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_int(0, 1)
        .add_op(create_op_aggregate_appending([
            create_op_seq(1, 6, 1).unwrap(),
            create_op_seq(6, 11, 1).unwrap(),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, (0..11).map(|v| v.to_string()).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn unset_field_value() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum_unbounded(0, 2, 1).unwrap())
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
fn unbounded_enum_backoff() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_aggregate([
            create_op_enum_unbounded(0, 1, 1).unwrap(),
            create_op_enum_unbounded(1, 3, 1).unwrap(),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1", "2"]);
    Ok(())
}

#[test]
fn unset_field_value_in_forkeach() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op_with_key("foo", create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_enum_unbounded(0, 2, 1).unwrap(),
            create_op_format("{foo:?}: {}").unwrap(),
            create_op_string_sink(&ss),
        ]))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        &["0: 0", "undefined: 1", "1: 0", "undefined: 1"]
    );
    Ok(())
}

#[test]
fn unset_field_value_debug_repr_is_undefined() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum_unbounded(0, 2, 1).unwrap())
        .add_op(create_op_format("{foo:?}_{}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["\"x\"_0", "undefined_1"]);
    Ok(())
}

#[test]
fn unset_field_value_does_not_trigger_underflow() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("x", 1)
        .add_op(create_op_key("x".to_owned()))
        .set_batch_size(1)
        .unwrap()
        .add_op(create_op_enum_unbounded(0, 4, 1).unwrap())
        .add_op(create_op_format("{x:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["\"x\"", "undefined", "undefined", "undefined"]);
    Ok(())
}

#[test]
fn seq_enum() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .push_str("x", 3)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum(0, 5, 1).unwrap())
        .add_op(create_op_format("{foo}{}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["x0", "x1", "x2"]);
    Ok(())
}

#[test]
fn double_drop() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(5)
        .unwrap()
        .add_op(create_op_seq(0, 15, 1).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_regex("1.*").unwrap())
        .add_op(create_op_format("{a}").unwrap())
        .add_op(create_op_regex(".*1.*").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "10", "11", "12", "13", "14"]);
    Ok(())
}

#[test]
fn select() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(5)
        .unwrap()
        .add_op_with_key(
            "a",
            create_op_literal_n(Literal::Text("foo".to_owned()), 3),
        )
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("a".to_owned()))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo", "foo"]);
    Ok(())
}

#[test]
fn select_after_key() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(5)
        .unwrap()
        .add_op(create_op_literal(Literal::Text("foo".to_owned())))
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("a".to_owned()))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo"]);
    Ok(())
}

#[test]
fn basic_cow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_str("123", 1)
        .add_op(create_op_fork([[
            create_op_regex_with_opts(
                ".",
                RegexOptions {
                    multimatch: true,
                    ..Default::default()
                },
            )?,
            create_op_string_sink(&ss),
        ]])?)
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(<SettingBatchSize as ChainSetting>::DEFAULT)]
fn cow_not_affecting_original(
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .push_str("123", 1)
        .add_op(create_op_fork([
            vec![
                create_op_regex_with_opts(
                    ".",
                    RegexOptions {
                        multimatch: true,
                        ..Default::default()
                    },
                )?,
                create_op_string_sink(&ss1),
            ],
            vec![create_op_string_sink(&ss2)],
        ])?)
        .run_collect_stringified()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["123"]);
    Ok(())
}

#[test]
fn chained_streams() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .set_batch_size(2)?
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op(create_op_aggregate_appending([
            create_op_file_reader_custom(
                Box::new(SliceReader::new("bar".as_bytes())),
                0,
            ),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar"]);
    Ok(())
}
#[test]
fn tf_literal_yields_to_cont() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_int_n(1, 3))
        .add_op(create_op_aggregate([
            create_op_str("foo"),
            create_op_str("bar"),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar", "bar"]);
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
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(stream_buffer_size)?
        .add_op(create_op_int_n(1, 3))
        .add_op(create_op_aggregate([
            create_op_file_reader_custom(
                Box::new(SliceReader::new(b"foo")),
                0,
            ),
            create_op_file_reader_custom(
                Box::new(SliceReader::new(b"bar")),
                0,
            ),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar", "bar"]);
    Ok(())
}

#[test]
fn error_on_sbs_0() {
    assert_eq!(
        ContextBuilder::without_exts()
            .set_stream_buffer_size(0)
            .err()
            .unwrap(),
        SettingConversionError {
            message: "value for setting %sbs cannot be zero".to_string(),
        }
    );
}
#[test]
fn negative_seq() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(-1, -5, -2)?)
        .run_collect_as::<i64>()?;
    assert_eq!(res, [-1, -3]);
    Ok(())
}

#[test]
// regression test against 49544e93b2d6ae40b61a2e2794063e9b9112cdee
// (inserter reservation issue on non fast step sequences)
fn negative_seq_stringified() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(-1, -5, -2)?)
        .run_collect_stringified()?;
    assert_eq!(res, ["-1", "-3"]);
    Ok(())
}

#[test]
fn stream_error_after_regular_error() -> Result<(), ScrError> {
    // TODO: this test used to test for a stream value error as output of
    // the format. that is no longer observed since join outputs a stream
    // so format receives an incomplete stream. We should make a test to
    // observe that again.
    // NOTE(cmrs): This sucks. Format gets the stream and starts outputting,
    // then the stream errors but it already started outputting so
    // it errors its own stream instead of debug printing the incoming error.
    // Maybe we should have a special case if format's output size so far
    // was zero?
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_stream_buffer_size(2)?
        .set_stream_size_threshold(3)?
        .add_op(create_op_error("A"))
        .add_op(create_op_file_reader_custom(
            Box::new(ErroringStream::new(5, SliceReader::new(b"BBBBB"))),
            0,
        ))
        .add_op(create_op_join_str("", 1))
        .add_op(create_op_format("{:??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        ["ERROR: in op id 1: ErroringStream: Error"]
    );
    Ok(())
}

#[test]
fn single_operator() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 1000, 1).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, int_sequence_strings(0..1000));
    Ok(())
}

#[test]
fn big_sum() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3000, 1).unwrap())
        .add_op(create_op_sum())
        .run_collect_as::<i64>()
        .unwrap();
    assert_eq!(res, &[4498500]);
    Ok(())
}

#[test]
fn seq_with_changing_str_length() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(1, 11, 1).unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, (1..11).map(|v| v.to_string()).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn basic_input_feeder() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
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
        .run_collect_stringified()?;
    assert_eq!(res, ["2"]);
    Ok(())
}

#[test]
fn field_refs_in_nopc() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
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
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "3"]);
    Ok(())
}

#[test]
fn basic_batching() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("1234"))
        .set_batch_size(2)
        .unwrap()
        .add_op(create_op_chars())
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3", "4"]);
    Ok(())
}

#[test]
fn basic_batched_head() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("1234"))
        .add_op(create_op_chars())
        .add_op(create_op_tail(3))
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "3", "4"]);
    Ok(())
}

#[rstest]
#[case(1, 3)]
#[case(2, 3)]
#[case(3, 3)]
#[case(3, 7)]
fn dup_into_sum(
    #[case] batch_size: usize,
    #[case] seq_len: i64,
) -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seqn(1, seq_len, 1).unwrap())
        .add_op(create_op_dup(2))
        .add_op(create_op_sum())
        .run_collect_stringified()?;
    assert_eq!(res, [(seq_len * (seq_len + 1)).to_string()]);
    Ok(())
}

#[test]
fn stream_error_into_print() -> Result<(), ScrError> {
    // TODO: this should work with a non zero offset too.
    // we have to make print take two streams. stdout and stderr respectively
    // it should then have options about where / if to report errors
    let offfset = 0;
    let print_target = MutexedWriteableTargetOwner::<Vec<u8>>::default();
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(1)?
        .set_stream_size_threshold(1)?
        .add_op(create_op_file_reader_custom(
            Box::new(ErroringStream::new(
                offfset,
                SliceReader::new("foo".as_bytes()),
            )),
            0,
        ))
        .add_op(create_op_print_with_opts(
            print_target.create_target(),
            PrintOptions {
                ignore_nulls: false,
                propagate_errors: false,
            },
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["null"]);
    assert_eq!(
        &*print_target.get(),
        b"ERROR: in op id 0: ErroringStream: Error\n"
    );
    Ok(())
}
