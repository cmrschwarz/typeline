use scr::{
    cli::CliOptions, options::context_builder::ContextBuilder,
    scr_error::ContextualizedScrError, CliOptionsWithDefaultExts,
};

#[test]
fn seq_sum() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_exts(),
        ["seq=10", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn empty_foreach_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_exts(),
        ["seq=10", "fe:", "end", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn foreach_block_no_colon() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_exts(),
        ["seq=10", "fe", "end", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn simple_foreach_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_exts(),
        ["str@foo=foo", "fc:", "nop", "next", "nop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}
