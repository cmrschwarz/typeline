use scr::{
    cli::CliOptions, options::context_builder::ContextBuilder,
    scr_error::ContextualizedScrError, CliOptionsWithDefaultExts,
};

#[test]
fn join_size_one_groups() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_exts(),
        ["seq=10", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}
