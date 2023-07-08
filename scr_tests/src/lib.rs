#![cfg(test)]

use std::io::Read;

use scr::{
    field_data::{push_interface::PushInterface, record_set::RecordSet},
    operations::{
        file_reader::create_op_file_reader_custom,
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn string_sink() -> Result<(), ScrError> {
    let ss = StringSinkHandle::new();
    ContextBuilder::default()
        .push_str("foo", 1)
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get().as_slice(), ["foo"]);
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
    assert_eq!(ss.get().as_slice(), ["foo", "bar"]);
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
    assert_eq!(ss.get().as_slice(), ["foo", "bar", "baz"]);
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
    assert_eq!(ss1.get().as_slice(), ["foo", "bar", "baz"]);
    assert_eq!(ss2.get().as_slice(), ["foo", "baz"]);
    Ok(())
}

#[test]
fn large_batch() -> Result<(), ScrError> {
    let number_string_list: Vec<_> = (0..10000).into_iter().map(|n| n.to_string()).collect();
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
    assert_eq!(ss.get().as_slice(), &number_string_list[0..1000]);
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
        .add_op(create_op_file_reader_custom(Box::new(TestStream {
            total_size: SIZE,
        })))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get().as_slice(),
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
    assert_eq!(ss.get().as_slice(), ["0", "1", "2"]);
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
    assert_eq!(ss.get().as_slice(), ["a", "c"]);
    Ok(())
}
