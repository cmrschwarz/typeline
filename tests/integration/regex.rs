use rstest::rstest;
use scr_core::{
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
    scr_error::ScrError,
    utils::{int_string_conversions::i64_to_str, test_utils::SliceReader},
};
use scr_ext_utils::dup::create_op_dup;

#[test]
fn lines_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
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
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str("foo\nbar\nbaz\n", 1)
        .add_op(create_op_regex_lines())
        .add_op_transparent(create_op_string_sink(&ss1))
        .add_op(create_op_regex(".*[^r]$").unwrap())
        .add_op(create_op_string_sink(&ss2))
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["foo", "bar", "baz"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["foo", "baz"]);
    Ok(())
}
#[test]
fn chained_multimatch_regex() -> Result<(), ScrError> {
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
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str(&number_string_joined, 1)
        .set_batch_size(10)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^[0-6]$").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &number_string_list[0..PASS]
    );
    Ok(())
}

#[test]
fn chained_regex_over_input() -> Result<(), ScrError> {
    let mut cb = ContextBuilder::default();
    for i in 0..3 {
        cb = cb.push_int(i, 1);
    }
    let ss = StringSinkHandle::default();
    cb.set_batch_size(1)
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_regex("[02]").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["0", "2"]);
    Ok(())
}

#[test]
fn large_batch() -> Result<(), ScrError> {
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
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .push_str(&number_string_joined, 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^[0-9]{1,3}$").unwrap())
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
fn large_batch_seq(
    #[case] count: i64,
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let re = regex::Regex::new(r"\d{1,3}").unwrap();
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .add_op(create_op_seq(0, count, 1).unwrap())
        .add_op(create_op_regex(r"\d{1,3}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    // large output -> no assert_eq!
    assert!(
        ss.get_data().unwrap().as_slice()
            == (0..count)
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
fn multi_batch_seq_with_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    const COUNT: usize = 6;
    ContextBuilder::default()
        .set_batch_size(COUNT / 2)
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,2}$").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        (0..COUNT).map(|i| i.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn large_seq_with_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    const COUNT: usize = 10000;
    ContextBuilder::default()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("^\\d{1,3}$").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    // large output -> no assert_eq!
    assert!(
        ss.get_data().unwrap().as_slice()
            == (0..1000).map(|i| i.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn stream_into_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
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
fn optional_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(9, 12, 1).unwrap())
        .add_op_appending(create_op_seq(20, 22, 1).unwrap())
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
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        [
            "9: null",
            "10: \"1\"",
            "11: \"1\"",
            "11: \"1\"",
            "20: null",
            "21: \"1\""
        ],
        ss.get_data().unwrap().as_slice(),
    );
    Ok(())
}

#[test]
fn zero_length_regex_match() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("babab", 1))
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
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), &["", "a", "", "a", "", ""]);
    Ok(())
}

#[rstest]
#[case("o*", "foo", &["", "oo", "o", ""])]
#[case("a.{2}", "aba34jf baacdaab", &["aba", "a34", "aac", "acd", "aab"])]
fn regex_match_overlapping(
    #[case] re: &str,
    #[case] input: &str,
    #[case] outputs: &[&'static str],
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str(input, 1))
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
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().data.as_slice(), outputs);
    Ok(())
}

#[test]
fn seq_into_regex_drop_unless_seven() -> Result<(), ScrError> {
    const COUNT: usize = 100000;
    let res: Vec<&str> = (0..COUNT)
        .filter_map(|v| {
            if v.to_string().contains('7') {
                Some("7")
            } else {
                None
            }
        })
        .collect();
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("7").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()
        .unwrap();
    // no assert_eq! because the output is large
    assert!(ss.get_data().unwrap().as_slice() == res);
    Ok(())
}

#[test]
fn regex_appending_without_input() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_appending(create_op_seq(1, 11, 1).unwrap())
        .add_op_appending(create_op_regex("[24680]").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(1..11).map(|v| v.to_string()).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn double_regex() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(0, 20, 1).unwrap())
        .add_op(create_op_regex(".*2.*").unwrap())
        .add_op(create_op_regex(".*").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), &["2", "12"]);
    Ok(())
}

#[test]
fn multimatch_after_dup() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
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
