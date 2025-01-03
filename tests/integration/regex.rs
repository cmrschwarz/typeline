use rstest::rstest;
use typeline::operators::transparent::create_op_transparent;
use typeline_core::{
    operators::{
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        literal::create_op_str,
        regex::{
            create_op_regex, create_op_regex_lines, create_op_regex_with_opts,
            RegexOptions,
        },
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
    utils::{int_string_conversions::i64_to_str, test_utils::SliceReader},
};
use typeline_ext_utils::{dup::create_op_dup, string_utils::create_op_lines};

#[test]
fn lines_regex() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .push_str("foo\nbar\nbaz", 1)
        .add_op(create_op_regex_lines())
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar", "baz"]);
    Ok(())
}

#[test]
fn index_capture_group() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .push_str("123", 1)
        .add_op(create_op_regex("(.)(.)(.)")?)
        .add_op(create_op_format("{}-{3}{2}{1}")?)
        .run_collect_stringified()?;
    assert_eq!(res, ["123-321"]);
    Ok(())
}

#[test]
fn regex_drop() -> Result<(), TypelineError> {
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .push_str("foo\nbar\nbaz", 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_transparent(create_op_string_sink(&ss1)))
        .add_op(create_op_regex(".*[^r]$").unwrap())
        .add_op(create_op_string_sink(&ss2))
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["foo", "bar", "baz"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["foo", "baz"]);
    Ok(())
}
#[test]
fn chained_multimatch_regex() -> Result<(), TypelineError> {
    const COUNT: usize = 20;
    const PASS: usize = 7;
    let number_string_list: Vec<_> =
        (0..COUNT).map(|n| n.to_string()).collect();
    let number_string_joined =
        number_string_list.iter().fold(String::new(), |mut f, n| {
            f.push_str(n.to_string().as_str());
            f.push('\n');
            f
        });
    let res = ContextBuilder::without_exts()
        .push_str(&number_string_joined, 1)
        .set_batch_size(10)
        .unwrap()
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^[0-6]$").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, number_string_list[0..PASS]);
    Ok(())
}

#[test]
fn chained_regex_over_input() -> Result<(), TypelineError> {
    let mut cb = ContextBuilder::without_exts();
    for i in 0..3 {
        cb = cb.push_int(i, 1);
    }
    let res = cb
        .set_batch_size(1)?
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_regex("[02]").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["0", "2"]);
    Ok(())
}

#[test]
fn large_batch() -> Result<(), TypelineError> {
    const COUNT: usize = 10000;
    const PASS: usize = 1000;
    let number_string_list: Vec<_> =
        (0..COUNT).map(|n| n.to_string()).collect();
    let number_string_joined =
        number_string_list.iter().fold(String::new(), |mut f, n| {
            f.push_str(n.to_string().as_str());
            f.push('\n');
            f
        });
    let res = ContextBuilder::without_exts()
        .push_str(&number_string_joined, 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^[0-9]{1,3}$").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, number_string_list[0..PASS]);
    Ok(())
}
#[rstest]
#[case(10000, 1)]
#[case(10000, 3)]
#[case(10000, 10000 - 1)]
#[case(10000, 10000 + 1)]
fn large_batch_seq(
    #[case] count: i64,
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let re = regex::Regex::new(r"\d{1,3}").unwrap();
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seq(0, count, 1).unwrap())
        .add_op(create_op_regex(r"\d{1,3}").unwrap())
        .run_collect_stringified()?;
    // large output -> no `assert_eq!`
    assert!(
        res == (0..count)
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
fn multi_batch_seq_with_regex() -> Result<(), TypelineError> {
    const COUNT: usize = 6;
    let res = ContextBuilder::without_exts()
        .set_batch_size(COUNT / 2)
        .unwrap()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,2}$").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, (0..COUNT).map(|i| i.to_string()).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn large_seq_with_regex() -> Result<(), TypelineError> {
    const COUNT: usize = 10000;
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,3}$").unwrap())
        .run_collect_stringified()?;
    // large output -> no assert_eq!
    assert!(res == (0..1000).map(|i| i.to_string()).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn stream_into_regex() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_stream_buffer_size(1)?
        .add_op(create_op_file_reader_custom(
            Box::new(SliceReader {
                data: "1\n2\n3\n".as_bytes(),
            }),
            0,
        ))
        .add_op(create_op_lines())
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3"]);
    Ok(())
}

#[test]
fn optional_regex() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_aggregate([
            create_op_seq(9, 12, 1).unwrap(),
            create_op_seq(20, 22, 1).unwrap(),
        ])
        .add_op(create_op_key("n".to_string()))
        .add_op(
            create_op_regex_with_opts(
                "1",
                RegexOptions {
                    non_mandatory: true,
                    multimatch: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .add_op(create_op_format("{n}: {:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(
        res,
        [
            "9: null",
            "10: \"1\"",
            "11: \"1\"",
            "11: \"1\"",
            "20: null",
            "21: \"1\""
        ],
    );
    Ok(())
}

#[test]
fn zero_length_regex_match() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("babab"))
        .add_op(
            create_op_regex_with_opts(
                "a*",
                RegexOptions {
                    multimatch: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .run_collect_stringified()?;
    assert_eq!(res, &["", "a", "", "a", "", ""]);
    Ok(())
}

#[rstest]
#[case("o*", "foo", &["", "oo", "o", ""])]
#[case("a.{2}", "aba34jf baacdaab", &["aba", "a34", "aac", "acd", "aab"])]
fn regex_match_overlapping(
    #[case] re: &str,
    #[case] input: &str,
    #[case] outputs: &[&'static str],
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str(input))
        .add_op(
            create_op_regex_with_opts(
                re,
                RegexOptions {
                    multimatch: true,
                    overlapping: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .run_collect_stringified()?;
    assert_eq!(res, outputs);
    Ok(())
}

#[rstest]
#[case(100, 200)]
#[case(1024, 5000)]
fn seq_into_regex_drop_unless_seven(
    #[case] batch_size: usize,
    #[case] count: usize,
) -> Result<(), TypelineError> {
    let expected: Vec<String> = (0..count)
        .filter_map(|v| {
            let v = v.to_string();
            if v.contains('7') {
                Some(v)
            } else {
                None
            }
        })
        .collect();
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)?
        .add_op(create_op_seq(0, count as i64, 1).unwrap())
        .add_op(create_op_regex(".*7.*").unwrap())
        .run_collect_stringified()?;
    // no assert_eq! because the output is large
    assert!(res == expected);
    Ok(())
}

#[test]
fn double_regex() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 20, 1).unwrap())
        .add_op(create_op_regex(".*2.*").unwrap())
        .add_op(create_op_regex(".*").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, &["2", "12"]);
    Ok(())
}

#[test]
fn full_match() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .push_str("foo\nbar", 1)
        .add_op(create_op_lines())
        .add_op(
            create_op_regex_with_opts(
                ".",
                RegexOptions {
                    full: true,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .run_collect_stringified()?;
    assert_eq!(res, &["foo", "bar"]);
    Ok(())
}

#[test]
fn multimatch_after_dup() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_dup(2))
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
    assert_eq!(res, ["f", "o", "o", "f", "o", "o"]);
    Ok(())
}
