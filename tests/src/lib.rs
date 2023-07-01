#![cfg(test)]

use scr::{
    field_data::{fd_push_interface::FDPushInterface, record_set::RecordSet},
    operations::{
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
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
    let number_string_list: Vec<_> = (0..100000).into_iter().map(|n| n.to_string()).collect();
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
