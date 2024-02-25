use rstest::rstest;
use scr::operators::sequence::create_op_enum;
use scr_core::{
    operators::{
        fork::create_op_fork,
        literal::{create_op_int, create_op_str},
        regex::create_op_regex,
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
fn unlink_after_fork(#[case] batch_size: usize) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_fork())
        .add_op_appending(create_op_int(2))
        .add_op_appending(create_op_int(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2", "3"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn unlink_without_append_after_fork(
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_fork())
        .add_op(create_op_enum(0, 1, 1).unwrap())
        .add_op_appending(create_op_enum(1, 3, 1).unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2"]);
    Ok(())
}

#[test]
fn ref_iter_reading_form_cow() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seq(1, 11, 1).unwrap())
        .add_op(create_op_regex(".*[24680]$").unwrap())
        .add_op(create_op_fork())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        &(1..11)
            .filter(|i| i % 2 == 0)
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn fork_without_input() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_fork())
        .add_op(create_op_str("foo"))
        .add_op(create_op_string_sink(&ss))
        .add_op(create_op_str("bar"))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}
