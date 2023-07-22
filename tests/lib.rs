mod utils;

use std::borrow::Cow;

use rstest::rstest;

use scr::operators::errors::ChainSetupError;
use scr::operators::fork::create_op_fork;
use scr::operators::join::{create_op_join, create_op_join_str};
use scr::operators::literal::{
    create_op_bytes, create_op_error, create_op_int, create_op_str, create_op_stream_error,
};
use scr::operators::next::create_op_next;
use scr::operators::select::create_op_select;
use scr::operators::sequence::{create_op_enum, create_op_seqn};
use scr::options::chain_options::DEFAULT_CHAIN_OPTIONS;
use scr::utils::i64_to_str;
use scr::{
    field_data::{push_interface::PushInterface, record_set::RecordSet},
    operators::{
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        literal::{create_op_literal, Literal},
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

use crate::utils::{ErroringStream, SliceReader, TricklingStream};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("foo", 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn tf_literal() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::String("foo".to_owned()), None))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn counted_tf_literal() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::String("x".to_owned()), Some(3)))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["x", "x", "x"]);
    Ok(())
}

#[test]
fn multi_doc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("foo", 1)
        .push_str("bar", 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}

#[test]
fn lines_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("foo\nbar\nbaz\n", 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar", "baz"]);
    Ok(())
}

#[test]
fn regex_drop() -> Result<(), ScrError> {
    let ss1 = StringSinkHandle::new();
    let ss2 = StringSinkHandle::new();
    let mut rs = RecordSet::default();
    rs.push_str("foo\nbar\nbaz\n", 1, false, false);
    ContextBuilder::default()
        .set_input(rs)
        .add_op(create_op_regex_lines())
        .add_op_transparent(create_op_string_sink(&ss1))
        .add_op(create_op_regex(".*[^r]$", Default::default()).unwrap())
        .add_op(create_op_string_sink(&ss2))
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["foo", "bar", "baz"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["foo", "baz"]);
    Ok(())
}

#[test]
fn large_batch() -> Result<(), ScrError> {
    const COUNT: usize = 10000;
    const PASS: usize = 1000;
    let number_string_list: Vec<_> = (0..COUNT).into_iter().map(|n| n.to_string()).collect();
    let number_string_joined = number_string_list.iter().fold(String::new(), |mut f, n| {
        f.push_str(n.to_string().as_str());
        f.push_str("\n");
        f
    });
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str(&number_string_joined, 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^[0-9]{1,3}$", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &number_string_list[0..PASS]
    );
    Ok(())
}
#[rstest]
#[case(10000, 1)]
#[case(10000, 3)]
#[case(10000, 10000 - 1)]
#[case(10000, 10000 + 1)]
fn large_batch_seq(#[case] count: i64, #[case] batch_size: usize) -> Result<(), ScrError> {
    let re = regex::Regex::new(r"\d{1,3}").unwrap();
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .add_op(create_op_seq(0, count, 1).unwrap())
        .add_op(create_op_regex(r"\d{1,3}", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..count)
            .filter_map(|v| {
                let v = i64_to_str(false, v).to_string();
                re.captures(v.as_str())
                    .and_then(|v| v.get(0).map(|v| v.as_str().to_owned()))
            })
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn trickling_stream() -> Result<(), ScrError> {
    const SIZE: usize = 4096;

    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(3)
        .add_op(create_op_file_reader_custom(
            Box::new(TricklingStream::new("a".as_bytes(), SIZE)),
            0,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    //not using assert_eq here because the output is very large
    assert!(
        ss.get_data().unwrap().as_slice()
            == [std::iter::repeat("a").take(SIZE).collect::<String>()]
    );
    Ok(())
}

#[test]
fn sequence() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2"]);
    Ok(())
}

#[test]
fn in_between_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("a", 1)
        .push_str("b", 1)
        .push_str("c", 1)
        .add_op(create_op_regex("[^b]", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a", "c"]);
    Ok(())
}

#[test]
fn drops_surrounding_single_val() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_regex("[1]", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1"]);
    Ok(())
}
#[test]
fn drops_surrounding_range() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 8, 1).unwrap())
        .add_op(create_op_regex("[2-5]", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "4", "5"]);
    Ok(())
}

#[test]
fn multi_batch_seq_with_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    const COUNT: usize = 6;
    ContextBuilder::default()
        .set_batch_size(COUNT / 2)
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,2}$", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..COUNT)
            .into_iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn large_seq_with_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    const COUNT: usize = 10000;
    ContextBuilder::default()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,3}$", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..1000)
            .into_iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn key_with_fmt() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_literal(Literal::Int(42), None))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("foo: {foo}, bar: {bar}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["foo: 42, bar: 42"]);
    Ok(())
}

#[test]
fn chained_seq() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_int(0, 1)
        .add_op_appending(create_op_seq(1, 6, 1).unwrap())
        .add_op_appending(create_op_seq(6, 11, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..11)
            .into_iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn format_width_spec() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
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
fn unset_field_value() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_format("{foo}{}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        &["x0", "ERROR: in op id 2: Format Error"]
    );
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("Format Error") //TODO: better error message
    );
    Ok(())
}

#[test]
fn unset_field_value_debug_repr_is_null() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_format("{foo:?}{}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["\"x\"0", "null1"]);
    Ok(())
}

#[test]
fn nonexisting_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{foo}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("Format Error") //TODO: better error message
    );
    Ok(())
}
#[test]
fn nonexisting_format_width_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{:foo$}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().get_first_error_message(),
        Some("Format Error") //TODO: better error message
    );
    Ok(())
}

#[test]
fn seq_enum() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_enum(0, 5, 1).unwrap())
        .add_op(create_op_format("{foo}{}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["x0", "x1", "x2"]);
    Ok(())
}

#[test]
fn dup_between_format_and_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    let mut regex_opts = RegexOptions::default();
    regex_opts.multimatch = true;
    ContextBuilder::default()
        .set_batch_size(2)
        .push_str("xxx", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_regex(".", regex_opts).unwrap())
        .add_op(create_op_format("{foo}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    println!("{:?}", ss.get().data);
    assert_eq!(ss.get_data().unwrap().as_slice(), &["xxx", "xxx", "xxx"]);
    Ok(())
}

#[test]
fn stream_into_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "1\n2\n3\n".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_regex_lines())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1", "2", "3"]);
    Ok(())
}

#[test]
fn format_after_surrounding_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 10, 1).unwrap())
        .add_op(create_op_regex("[3-5]", RegexOptions::default()).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["3", "4", "5"]);
    Ok(())
}

#[test]
fn batched_format_after_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    const COUNT: i64 = 20;
    ContextBuilder::default()
        .set_batch_size(3)
        .add_op(create_op_seq(0, COUNT, 1).unwrap())
        .add_op(create_op_regex(".*[3].*", RegexOptions::default()).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(0..COUNT)
            .filter_map(|v| {
                let v = i64_to_str(false, v).to_string();
                if v.contains("3") {
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
fn double_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(5)
        .add_op(create_op_seq(0, 15, 1).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_regex("1.*", RegexOptions::default()).unwrap())
        .add_op(create_op_format("{a}".as_bytes()).unwrap())
        .add_op(create_op_regex(".*1.*", RegexOptions::default()).unwrap())
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
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(5)
        .add_op_with_opts(
            create_op_literal(Literal::String("foo".to_owned()), Some(3)),
            None,
            Some("a"),
            None,
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
    let ss = StringSinkHandle::new();
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
fn optional_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(9, 12, 1).unwrap())
        .add_op_appending(create_op_seq(20, 22, 1).unwrap())
        .add_op(create_op_key("n".to_string()))
        .add_op(
            create_op_regex(
                "1",
                RegexOptions {
                    optional: true,
                    multimatch: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .add_op(create_op_format("{n}: {:?}".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        [
            "9: null",
            "10: \"1\"",
            "11: \"1\"",
            "11: \"1\"",
            "20: null",
            "21: \"1\""
        ]
    );
    Ok(())
}

#[test]
fn stream_into_format() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "xxx".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_format("a -> {} -> b".as_bytes()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a -> xxx -> b"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn stream_into_multiple_different_formats(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
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
fn basic_cow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("123", 1)
        .add_op(create_op_fork())
        .add_op(
            create_op_regex(
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
#[case(DEFAULT_CHAIN_OPTIONS.default_batch_size.unwrap())]
fn cow_not_affecting_original(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss1 = StringSinkHandle::new();
    let ss2 = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .push_str("123", 1)
        .add_op(create_op_fork())
        .add_op(
            create_op_regex(
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
fn join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(1, 4, 1).unwrap())
        .add_op(create_op_join(Some(",".as_bytes().to_owned()), None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["1,2,3"]);
    Ok(())
}
#[test]
fn join_single() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(1, 2, 1).unwrap())
        .add_op(create_op_join(
            Some(",".as_bytes().to_owned()),
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
    let ss = StringSinkHandle::new();
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
    let ss = StringSinkHandle::new();
    /*   ContextBuilder::default()
        .add_op(create_op_join(None, None, true))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &[] as &[String]);*/
    ContextBuilder::default()
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), [""]);
    Ok(())
}

#[test]
fn join_no_sep() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
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
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(1)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new("bar".as_bytes())),
            0,
        ))
        .add_op(create_op_join(
            Some(", ".as_bytes().to_owned()),
            None,
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo, bar"]);
    Ok(())
}

#[test]
fn join_after_append() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
        .add_op_appending(create_op_str("bar", 1))
        .add_op(create_op_join_str(", ", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo, bar"]);
    Ok(())
}

#[test]
fn join_after_enum() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 2))
        .add_op(create_op_enum(0, i64::MAX, 1).unwrap())
        .add_op(create_op_join_str(",", 0))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0,1"]);
    Ok(())
}

#[test]
fn join_dropped_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_literal(Literal::Int(1), Some(1)))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(SliceReader::new("bar".as_bytes())),
            0,
        ))
        .add_op(create_op_join(
            Some(", ".as_bytes().to_owned()),
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
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(ErroringStream::new(2, SliceReader::new("bar".as_bytes()))),
            0,
        ))
        .add_op_appending(create_op_literal(Literal::Int(1), Some(1)))
        .add_op(create_op_join(
            Some(", ".as_bytes().to_owned()),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        ["ERROR: in op id 1: ErroringStream: Expected Debug Error"]
    );
    Ok(())
}

#[test]
fn stream_into_dup_into_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_batch_size(2)
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader::new("foo".as_bytes())),
            0,
        ))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_str("123", 1))
        .add_op(
            create_op_regex(
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
            Some(",".as_bytes().to_owned()),
            Some(3),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["foo,foo,foo"]);
    Ok(())
}

#[test]
fn chained_streams() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
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
    let ss = StringSinkHandle::new();
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
fn tf_file_yields_to_cont(#[case] stream_buffer_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
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
            .run(),
        Err(ScrError::ChainSetupError(ChainSetupError {
            message: Cow::Borrowed("stream buffer size cannot be zero"),
            chain_id: 0
        }))
    ));
}

#[test]
fn debug_format_surrounds_with_quotes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_bytes(b"bar", 0))
        .add_op_appending(create_op_error("baz", 0))
        .add_op(create_op_format(b"{:#?}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["\"foo\"", "'bar'", "!\"baz\""]
    );
    Ok(())
}

#[test]
fn more_debug_format_surrounds_with_quotes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_bytes(b"bar", 0))
        .add_op_appending(create_op_error("baz", 0))
        .add_op(create_op_format(b"{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["\"foo\"", "'bar'", "!\"baz\""]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_turns_into_stream(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .set_stream_size_threshold(2)
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_str("bar", 0))
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format(b"{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["~\"foo,bar\""]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
fn join_on_error(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .set_stream_size_threshold(2)
        .add_op(create_op_str("foo", 0))
        .add_op_appending(create_op_error("bar", 0))
        .add_op(create_op_join_str(",", 2))
        .add_op(create_op_format(b"{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), ["~!\"bar\""]);

    Ok(())
}

#[test]
fn error_formatting() -> Result<(), ScrError> {
    let pairs = [
        ("{:?}", "!\"ERROR: in op id 0: A\""),
        ("{:??}", "!\"ERROR: in op id 0: A\""),
        ("{:#??}", "!\"A\""),
        ("{:#??}", "!\"A\""),
    ];
    for (fmt, res) in pairs {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_error("A", 1))
            .add_op(create_op_format(fmt.as_bytes()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()?;
        assert_eq!(ss.get().data.as_slice(), [res]);
    }
    Ok(())
}

#[test]
fn stream_error_formatting() -> Result<(), ScrError> {
    let pairs = [
        ("{:?}", "!\"ERROR: in op id 0: A\""),
        ("{:??}", "~!\"ERROR: in op id 0: A\""),
        ("{:#?}", "!\"A\""),
        ("{:#??}", "~!\"A\""),
    ];
    for (fmt, res) in pairs {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_stream_error("A", 1))
            .add_op(create_op_format(fmt.as_bytes()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()?;
        assert_eq!(ss.get().data.as_slice(), [res]);
    }
    Ok(())
}

#[test]
fn stream_error_after_regular_error() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(2)
        .set_stream_size_threshold(3)
        .add_op(create_op_error("A", 0))
        .add_op_appending(create_op_file_reader_custom(
            Box::new(ErroringStream::new(5, SliceReader::new(b"BBBBB"))),
            0,
        ))
        .add_op(create_op_join_str("", 1))
        .add_op(create_op_format(b"{:#??}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        ["!\"A\"", "~!\"ErroringStream: Expected Debug Error\""]
    );
    Ok(())
}

#[test]
fn single_operator() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 10000, 1).unwrap())
        .run()?;
    assert_eq!(ss.get().data.as_slice(), &[] as &[String]);
    Ok(())
}
