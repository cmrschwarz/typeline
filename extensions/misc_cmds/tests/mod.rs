use scr_core::{
    operators::{
        select::create_op_select,
        sequence::create_op_seqn,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};
use scr_ext_misc_cmds::{head::create_op_head, primes::create_op_primes};

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
        .add_op_with_opts(create_op_primes(), None, Some("p"), false, false)
        .add_op(create_op_head(3))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "3", "5"]);
    Ok(())
}
