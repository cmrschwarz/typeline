use scr::{
    operators::{
        foreach::create_op_foreach,
        join::create_op_join,
        print::{create_op_print_with_opts, PrintOptions},
        sequence::create_op_seq,
    },
    options::session_setup::ScrSetupOptions,
    utils::test_utils::DummyWritableTarget,
};
use scr_core::{
    options::context_builder::ContextBuilder, scr_error::ScrError,
};
use scr_ext_utils::{
    exec::create_op_exec_from_strings,
    string_utils::{create_op_lines, create_op_trim},
};

use crate::integration::UTILS_EXTENSION_REGISTRY;

#[test]
fn parse_exec() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_extensions(UTILS_EXTENSION_REGISTRY.clone()),
        ["[", "exec", "sh", "-c", "sleep 0.1; echo foo", "]"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo\n"]);
    Ok(())
}

#[test]
fn parse_exec_stdin() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_extensions(UTILS_EXTENSION_REGISTRY.clone()),
        ["str=foo", "[", "exec", "{", "-i", "}", "cat", "]"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo"]);
    Ok(())
}

#[test]
fn parse_exec_2() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_extensions(UTILS_EXTENSION_REGISTRY.clone()),
        ["seq=3", "[", "exec", "echo", "{}", "]"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["0\n", "1\n", "2\n"]);
    Ok(())
}

#[test]
fn run_multi_exec() -> Result<(), ScrError> {
    let target = DummyWritableTarget::new();

    ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_exec_from_strings(["echo", "a"]).unwrap())
        .add_op(create_op_print_with_opts(
            target.get_target(),
            PrintOptions::default(),
        ))
        .run()?;
    assert_eq!(&**target.get(), "a\n\na\n\na\n\n");
    Ok(())
}

#[test]
fn run_sleep() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 6, 2).unwrap())
        .add_op(create_op_foreach([
            create_op_exec_from_strings([
                "sh",
                "-c",
                "sleep 0.{} && echo 0.{}",
            ])
            .unwrap(),
            create_op_trim(),
        ]))
        //  .set_debug_log_path("run_sleep.html")?
        .run_collect_stringified()?;
    assert_eq!(&res, &["0.0", "0.2", "0.4"]);
    Ok(())
}

#[test]
fn run_exec_into_join() -> Result<(), ScrError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, 3, 1).unwrap())
        .add_op(create_op_foreach([
            create_op_exec_from_strings(["sh", "-c", "yes | head -n 70"])
                .unwrap(),
            create_op_lines(),
            create_op_join(None, None, false),
        ]))
        .run_collect_stringified()?;
    assert_eq!(&res, &[
        "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
    ]);
    Ok(())
}
