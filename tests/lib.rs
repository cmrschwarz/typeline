use scr::bstr::ByteSlice;

use scr::operations::sequence::{create_op_enum, create_op_seq_append};
use scr::utils::i64_to_str;
use scr::{
    field_data::{push_interface::PushInterface, record_set::RecordSet},
    operations::{
        data_inserter::{create_op_data_inserter, DataToInsert},
        file_reader::create_op_file_reader_custom,
        format::create_op_format,
        key::create_op_key,
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

use std::io::Read;

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
fn data_inserter() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_data_inserter(
            DataToInsert::String("foo".to_owned()),
            None,
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn counted_data_inserter() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .add_op(create_op_data_inserter(
            DataToInsert::String("x".to_owned()),
            Some(3),
            false,
        ))
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
        .add_op(create_op_string_sink(&ss1))
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
#[test]
fn large_batch_seq() -> Result<(), ScrError> {
    const COUNT: i64 = 10000;

    let re = regex::Regex::new(r"\d{1,3}").unwrap();
    for bs in [1, 3, COUNT - 1, COUNT, COUNT + 1] {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .set_batch_size(bs as usize)
            .add_op(create_op_seq(0, COUNT, 1).unwrap())
            .add_op(create_op_regex(r"\d{1,3}", RegexOptions::default()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()?;
        assert_eq!(
            ss.get_data().unwrap().as_slice(),
            &(0..COUNT)
                .filter_map(|v| {
                    let v = i64_to_str(false, v).to_string();
                    re.captures(v.as_str())
                        .and_then(|v| v.get(0).map(|v| v.as_str().to_owned()))
                })
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn trickling_stream() -> Result<(), ScrError> {
    const SIZE: usize = 4096;
    struct TestStream {
        total_size: usize,
    }

    impl Read for TestStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.total_size == 0 || buf.len() == 0 {
                return Ok(0);
            }
            buf[0] = 'a' as u8;
            self.total_size -= 1;
            return Ok(1);
        }
    }
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_stream_buffer_size(3)
        .add_op(create_op_file_reader_custom(
            Box::new(TestStream { total_size: SIZE }),
            true,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        [std::iter::repeat("a").take(SIZE).collect::<String>()]
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
        .add_op(create_op_data_inserter(DataToInsert::Int(42), None, true))
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("foo: {foo}, bar: {bar}".as_bytes().as_bstr()).unwrap())
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
        .add_op(create_op_seq_append(1, 6, 1).unwrap())
        .add_op(create_op_seq_append(6, 11, 1).unwrap())
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
        .add_op(create_op_format("{foo:~^bar$}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &["x", "x", "~x", "~x~", "~~x~", "~~x~~"]
    );
    Ok(())
}

#[test]
fn unset_value() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 1)
        .add_op(create_op_key("foo".to_owned()))
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_key("bar".to_owned()))
        .add_op(create_op_format("{foo}{bar}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().data.as_slice(),
        &["x0", "Error: in op id 3: Format Error"]
    );
    assert_eq!(
        ss.get().errors.get(&1).map(|v| (&*v.message)),
        Some("Format Error") //TODO: better error message
    );
    Ok(())
}

#[test]
fn nonexisting_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{foo}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().errors.get(&0).map(|v| (&*v.message)),
        Some("Format Error") //TODO: better error message
    );
    Ok(())
}
#[test]
fn nonexisting_format_width_key() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("x", 3)
        .add_op(create_op_format("{:foo$}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert!(ss.get_data().is_err());
    assert_eq!(
        ss.get().errors.get(&0).map(|v| (&*v.message)),
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
        .add_op(create_op_format("{foo}{}".as_bytes().as_bstr()).unwrap())
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
        .add_op(create_op_format("{foo}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    println!("{:?}", ss.get().data);
    assert_eq!(ss.get_data().unwrap().as_slice(), &["xxx", "xxx", "xxx"]);
    Ok(())
}

struct SliceReader<'a> {
    data: &'a [u8],
}

impl<'a> Read for SliceReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.data.len();
        if buf.len() >= len {
            buf[0..len].copy_from_slice(self.data);
            self.data = &[];
            return Ok(len);
        }
        let (lhs, rhs) = self.data.split_at(buf.len());
        buf.copy_from_slice(lhs);
        self.data = rhs;
        Ok(lhs.len())
    }
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
            true,
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
        .add_op(create_op_format("{a}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["3", "4", "5"]);
    Ok(())
}

#[test]
fn batched_format_after_drop() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .set_batch_size(1)
        .add_op(create_op_seq(0, 20, 1).unwrap())
        .add_op(create_op_regex(".*[3].*", RegexOptions::default()).unwrap())
        .add_op(create_op_key("a".to_owned()))
        .add_op(create_op_format("{a}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["3", "13"]);
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
        .add_op(create_op_format("{a}".as_bytes().as_bstr()).unwrap())
        .add_op(create_op_regex(".*1.*", RegexOptions::default()).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["1", "10", "11", "12", "13", "14"]
    );
    Ok(())
}
