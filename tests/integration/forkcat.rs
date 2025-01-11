use rstest::rstest;
use typeline::{
    operators::nop_copy::create_op_nop_copy, utils::maybe_text::MaybeText,
};
use typeline_core::{
    operators::{
        forkcat::create_op_forkcat,
        format::create_op_format,
        join::create_op_join,
        literal::create_op_str,
        nop::create_op_nop,
        regex::create_op_regex,
        sequence::{create_op_seq, create_op_seqn},
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    typeline_error::TypelineError,
};
use typeline_ext_utils::dup::create_op_dup;

#[test]
fn empty_forkcat() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_forkcat([[]]))
        .add_op(create_op_format("{:??}")?)
        .run_collect_stringified()?;
    assert_eq!(res, ["undefined"]);
    Ok(())
}

#[test]
fn nop_forkcat() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([[]]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo"]);
    Ok(())
}

#[test]
fn basic_forkcat() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_forkcat([
            [create_op_str("foo")],
            [create_op_str("bar")],
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "bar"]);
    Ok(())
}

#[test]
fn forkcat_with_input() -> Result<(), TypelineError> {
    let ss1 = StringSinkHandle::default();
    let ss2 = StringSinkHandle::default();
    ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([
            [create_op_string_sink(&ss1)],
            [create_op_string_sink(&ss2)],
        ]))
        .add_op(create_op_nop())
        .run()?;
    assert_eq!(ss1.get_data().unwrap().as_slice(), ["foo"]);
    assert_eq!(ss2.get_data().unwrap().as_slice(), ["foo"]);
    Ok(())
}

#[test]
fn forkcat_dup() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([[create_op_nop()], [create_op_nop()]]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_sandwiched_write() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([
            [create_op_nop()],
            [create_op_format("{}{}").unwrap()],
            [create_op_nop()],
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foofoo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_double_field_refs() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([
            [create_op_nop_copy()],
            [create_op_nop_copy()],
        ]))
        .add_op(create_op_nop_copy())
        .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn forkcat_into_join() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_str("foo"))
        .add_op(create_op_forkcat([[create_op_nop()], [create_op_nop()]]))
        .add_op(create_op_join(
            Some(MaybeText::from_bytes(b",")),
            None,
            false,
        ))
        .run_collect_stringified()?;
    assert_eq!(res, ["foo,foo"]);
    Ok(())
}

#[test]
fn forkcat_build_sql_insert() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_ops([
            create_op_forkcat([
                vec![create_op_str("INSERT INTO T VALUES ")],
                vec![
                    create_op_seq(0, 5, 1).unwrap(),
                    create_op_format("({})").unwrap(),
                    create_op_join(
                        Some(MaybeText::from_bytes(b", ")),
                        None,
                        false,
                    ),
                ],
                vec![create_op_str(";")],
            ]),
            create_op_join(None, None, false),
        ])
        .run_collect_stringified()?;
    assert_eq!(res, ["INSERT INTO T VALUES (0), (1), (2), (3), (4);"]);
    Ok(())
}

#[test]
fn forkcat_input_equals_named_var() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("a", create_op_str("a"))
        .add_op(create_op_forkcat([
            [create_op_format("{a}").unwrap()],
            [create_op_nop()],
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "a"]);
    Ok(())
}

#[test]
fn forkcat_surviving_vars() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("lbl", create_op_seq(0, 2, 1).unwrap())
        .add_op(create_op_forkcat([
            [create_op_str("a")],
            [create_op_str("b")],
        ]))
        // despite the fork, we should preserve non string access semanticss
        .add_op(create_op_format("{lbl:?}: {}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["0: a", "1: a", "0: b", "1: b"]);
    Ok(())
}

#[test]
fn forkcat_with_drop_in_sc() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_forkcat([
            [create_op_regex("2").unwrap()],
            [create_op_nop()],
        ]))
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["2123"]);
    Ok(())
}

#[test]
fn forkcat_with_batches() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(3)
        .unwrap()
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_forkcat([
            [create_op_regex("[24]").unwrap()],
            [create_op_dup(0)],
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "4"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(4)]
#[case(5)]
fn forkcat_with_batches_into_join(
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)?
        .add_op(create_op_seqn(1, 5, 1)?)
        .add_op(create_op_forkcat([
            [create_op_regex("[24]")?],
            [create_op_nop()],
        ]))
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["2412345"]);
    Ok(())
}

#[rstest]
#[case(1)]
#[case(2)]
#[case(3)]
#[case(10)]
fn forkcat_on_unapplied_commands(
    #[case] batch_size: usize,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_seqn(1, 5, 1).unwrap())
        .add_op(create_op_regex("[24]").unwrap())
        .add_op(create_op_forkcat([[create_op_nop()], [create_op_dup(0)]]))
        .add_op(create_op_join(None, None, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["24"]);
    Ok(())
}
