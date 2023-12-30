use scr::parse_cli_from_strings;
use scr_core::{
    operators::{
        select::create_op_select,
        sequence::create_op_seqn,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_misc_cmds::{
    head::create_op_head, primes::create_op_primes, tail::create_op_tail_add,
};

#[test]
fn primes() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_with_opts(create_op_primes(), None, Some("p"), false, false)
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_select("p".into()))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "5"]);
    Ok(())
}

#[test]
fn primes_head() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_primes())
        .add_op(create_op_head(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "5"]);
    Ok(())
}

#[test]
fn seq_tail_add() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_tail_add(7))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["8", "9", "10"]);
    Ok(())
}

#[test]
fn primes_head_tail_add() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_primes())
        .add_op(create_op_tail_add(3))
        .add_op(create_op_head(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["7", "11", "13"]);
    Ok(())
}

#[test]
fn head_tail_cli() -> Result<(), ScrError> {
    let sess_opts =
        parse_cli_from_strings(["scr", "primes", "tail=+3", "head=5"])?;
    let res = ContextBuilder::from_session_opts(sess_opts)
        .run_collect_as::<String>()?;
    assert_eq!(res, ["7", "11", "13"]);
    Ok(())
}
