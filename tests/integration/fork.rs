use rstest::rstest;
use typeline::operators::aggregate::create_op_aggregate_appending;
use typeline_core::{
    operators::{
        fork::create_op_fork,
        literal::{create_op_int, create_op_str},
        regex::create_op_regex,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
};
use typeline_ext_utils::sequence::create_op_seq;

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
fn unlink_after_fork(#[case] batch_size: usize) -> Result<(), TypelineError> {
    use typeline_ext_utils::sequence::create_op_seq;

    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_fork(vec![vec![
            create_op_aggregate_appending([
                create_op_int(2),
                create_op_int(3),
            ]),
            create_op_string_sink(&ss),
        ]])?)
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2", "3"]);
    Ok(())
}

// TODO: the aggregator needs a redesign. it does currently not
// work well with groups
#[cfg(any())]
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
fn unlink_without_append_after_fork(
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_fork_with_opts(vec![vec![
            (
                OperatorBaseOptions::default(),
                create_op_enum(0, 1, 1).unwrap(),
            ),
            (
                OperatorBaseOptions {
                    append_mode: true,
                    ..Default::default()
                },
                create_op_enum(1, 3, 1).unwrap(),
            ),
            (OperatorBaseOptions::default(), create_op_string_sink(&ss)),
        ]])?)
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["0", "1", "2"]);
    Ok(())
}

#[test]
fn ref_iter_reading_form_cow() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_seq(1, 11, 1).unwrap())
        .add_op(create_op_regex(".*[24680]$").unwrap())
        .add_op(create_op_fork([[create_op_string_sink(&ss)]])?)
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
fn fork_without_input() -> Result<(), TypelineError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_fork([
            [create_op_str("foo"), create_op_string_sink(&ss)],
            [create_op_str("bar"), create_op_string_sink(&ss)],
        ])?)
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}
