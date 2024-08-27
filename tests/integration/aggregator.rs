use scr::operators::{
    aggregator::create_op_aggregate_appending,
    fork::create_op_fork,
    string_sink::{create_op_string_sink, StringSinkHandle},
};
use scr_core::{
    operators::{
        literal::{create_op_int, create_op_str},
        sequence::create_op_seqn,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::int_sequence_strings,
};

#[test]
fn simple_aggregate() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op_aggregate([create_op_str("foo"), create_op_str("bar")])
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar"]);
    Ok(())
}

#[test]
fn batched_aggregate() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .add_op_aggregate([
            create_op_seqn(1, 10, 1).unwrap(),
            create_op_seqn(11, 15, 1).unwrap(),
        ])
        .run_collect_stringified()?;
    assert_eq!(res, (1..16).map(|i| i.to_string()).collect::<Vec<String>>());
    Ok(())
}

#[test]
fn append_after_fork() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::without_exts()
        //.set_batch_size(2)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(
            create_op_fork(vec![vec![
                create_op_aggregate_appending([create_op_int(4)]),
                create_op_string_sink(&ss),
            ]])
            .unwrap(),
        )
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        int_sequence_strings(1..5)
    );
    Ok(())
}

// TODO: this feature is insane. figure out something better
// #[test]
// fn parse_aggregation_across_fork() -> Result<(), ScrError> {
// let sess_opts = parse_cli_from_strings([
// "scr", "seqn=10", "forkcat", "+int=11", "r=.*",
// ])?;
// let res = ContextBuilder::from_session_opts(sess_opts)
// .run_collect_stringified()?;
// assert_eq!(res, int_sequence_strings(1..12));
// Ok(())
// }
