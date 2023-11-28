use scr_core::{
    operators::{
        forkcat::create_op_forkcat,
        format::{create_op_format, create_op_format_from_str},
        join::create_op_join,
        literal::create_op_str,
        next::create_op_next,
        nop::create_op_nop,
        regex::create_op_regex,
        sequence::{create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
        up::create_op_up,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn basic_forkcat() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_forkcat())
        .add_op(create_op_str("foo", 1))
        .add_op(create_op_next())
        .add_op(create_op_str("bar", 1))
        .add_op(create_op_up(1))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar"]);
    Ok(())
}

#[test]
fn forkcat_with_input() -> Result<(), ScrError> {
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
        .add_op(create_op_forkcat())
        .add_op(create_op_string_sink(&ss1))
        .add_op(create_op_next())
        .add_op(create_op_string_sink(&ss2))
        .add_op(create_op_up(1))
        .add_op(create_op_nop())
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["foo"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn forkcat_dup() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_sandwiched_write() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_format(b"{}{}").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "foofoo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_into_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo", 1))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_join(Some(b",".to_vec()), None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo,foo"]);
    Ok(())
}

#[test]
fn forkcat_build_sql_insert() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_ops([
            create_op_forkcat(),
            create_op_str("INSERT INTO T VALUES ", 1),
            create_op_next(),
            create_op_seq(0, 5, 1).unwrap(),
            create_op_format_from_str("({})").unwrap(),
            create_op_join(Some(b", ".to_vec()), None, false),
            create_op_next(),
            create_op_str(";", 1),
            create_op_up(1),
            create_op_join(None, None, false),
            create_op_string_sink(&ss),
        ])
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["INSERT INTO T VALUES (0), (1), (2), (3), (4);"]
    );
    Ok(())
}

#[test]
fn forkcat_input_equals_named_var() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_with_label(create_op_str("a", 1), "a")
        .add_op(create_op_forkcat())
        .add_op(create_op_format(b"{a}").unwrap())
        .add_op(create_op_up(1))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a"]);
    Ok(())
}

#[test]
fn forkcat_surviving_vars() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_with_label(create_op_seq(0, 2, 1).unwrap(), "lbl")
        .add_op(create_op_forkcat())
        .add_op(create_op_str("a", 0))
        .add_op(create_op_next())
        .add_op(create_op_str("b", 0))
        .add_op(create_op_up(1))
        .add_op(create_op_format(b"{lbl:?}: {}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["0: a", "1: a", "0: b", "1: b"]
    );
    Ok(())
}

#[test]
fn forkcat_with_drop_in_sc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_regex("2").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2123"]);
    Ok(())
}

#[test]
fn forkcat_with_batches() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2412345"]);
    Ok(())
}

#[test]
fn forkcat_on_unapplied_commands() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(1)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_up(1))
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["24"]);
    Ok(())
}
