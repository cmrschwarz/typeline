use scr::operators::aggregator::create_op_aggregate;
use scr_core::{
    operators::{
        fork::create_op_fork,
        literal::{create_op_int, create_op_str},
        sequence::create_op_seqn,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::int_sequence_strings,
};

#[test]
fn simple_aggregate() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_aggregate([create_op_str("foo"), create_op_str("bar")])
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}

#[test]
fn batched_aggregate() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .add_op_aggregate([
            create_op_seqn(1, 10, 1).unwrap(),
            create_op_seqn(11, 15, 1).unwrap(),
        ])
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        (1..16).map(|i| i.to_string()).collect::<Vec<String>>()
    );
    Ok(())
}

#[test]
fn append_after_fork() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        //.set_batch_size(2)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(
            create_op_fork([[create_op_aggregate([
                create_op_int(4),
                create_op_string_sink(&ss),
            ])]])
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
