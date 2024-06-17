#![allow(unused_imports)] // TODO
use rstest::rstest;
use scr::{
    cli::CliOptions,
    parse_cli_from_strings,
    utils::{maybe_text::MaybeText, test_utils::int_sequence_strings},
};
use scr_core::{
    operators::{
        end::create_op_end,
        forkcat::create_op_forkcat,
        format::{create_op_format, create_op_format_from_str},
        join::create_op_join,
        literal::create_op_str,
        next::create_op_next,
        nop::create_op_nop,
        regex::create_op_regex,
        sequence::{create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[test]
fn basic_forkcat() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_forkcat())
        .add_op(create_op_str("foo"))
        .add_op(create_op_next())
        .add_op(create_op_str("bar"))
        .add_op(create_op_end())
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
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat())
        .add_op(create_op_string_sink(&ss1))
        .add_op(create_op_next())
        .add_op(create_op_string_sink(&ss2))
        .add_op(create_op_end())
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
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_sandwiched_write() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_format("{}{}").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "foofoo", "foo"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_into_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            None,
            false,
        ))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["foo,foo"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_build_sql_insert() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_ops([
            create_op_forkcat(),
            create_op_str("INSERT INTO T VALUES "),
            create_op_next(),
            create_op_seq(0, 5, 1).unwrap(),
            create_op_format_from_str("({})").unwrap(),
            create_op_join(Some(MaybeText::from_bytes(b", ")), None, false),
            create_op_next(),
            create_op_str(";"),
            create_op_end(),
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

#[cfg(any())] // TODO
#[test]
fn forkcat_input_equals_named_var() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_with_label(create_op_str("a"), "a")
        .add_op(create_op_forkcat())
        .add_op(create_op_format("{a}").unwrap())
        .add_op(create_op_end())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["a"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_surviving_vars() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op_with_label(create_op_seq(0, 2, 1).unwrap(), "lbl")
        .add_op(create_op_forkcat())
        .add_op(create_op_str("a"))
        .add_op(create_op_next())
        .add_op(create_op_str("b"))
        .add_op(create_op_end())
        .add_op(create_op_format("{lbl:?}: {}").unwrap())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(
        ss.get_data().unwrap().as_slice(),
        ["0: a", "1: a", "0: b", "1: b"]
    );
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_with_drop_in_sc() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_regex("2").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2123"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_with_batches() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(3)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_end())
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2", "4"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn forkcat_with_batches_into_join() -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(2)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_next())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["2412345"]);
    Ok(())
}

#[cfg(any())] // TODO
#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(10)]
fn forkcat_on_unapplied_commands(
    #[case] batch_size: usize,
) -> Result<(), ScrError> {
    let ss = StringSinkHandle::default();
    ContextBuilder::default()
        .set_batch_size(batch_size)
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_forkcat())
        .add_op(create_op_nop())
        .add_op(create_op_end())
        .add_op(create_op_join(None, None, false))
        .add_op(create_op_string_sink(&ss))
        .run()?;
    assert_eq!(ss.get_data().unwrap().as_slice(), ["24"]);
    Ok(())
}

#[cfg(any())] // TODO
#[test]
fn parse_forkcat() -> Result<(), ScrError> {
    let sess_opts = parse_cli_from_strings(
        CliOptions::default(),
        ["scr", "seqn=10", "forkcat", "r=.*", "end"],
    )?;
    let res = ContextBuilder::from_session_opts(sess_opts)
        .run_collect_stringified()?;
    assert_eq!(res, int_sequence_strings(1..11));
    Ok(())
}
