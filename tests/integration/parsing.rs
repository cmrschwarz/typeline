use scr::{
    cli::{call_expr::Span, CliOptions},
    operators::errors::OperatorCreationError,
    options::context_builder::ContextBuilder,
    scr_error::{ContextualizedScrError, ScrError},
    CliOptionsWithDefaultExtensions,
};

#[test]
fn seq_sum() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_extensions(),
        ["seq=10", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn empty_foreach_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_extensions(),
        ["seq=10", "fe:", "end", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn foreach_block_no_colon() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_extensions(),
        ["seq=10", "fe", "end", "sum"],
    );
    assert_eq!(
        res.err().map(|e| e.err),
        Some(ScrError::OperationCreationError(
            OperatorCreationError::new(
                "unknown operator 'end'",
                Span::from_cli_arg(2, 3, 0, 3)
            )
        ))
    );
    Ok(())
}

#[test]
fn foreach_no_block() -> Result<(), ContextualizedScrError> {
    // TODO: probably emit an error for this
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_extensions(),
        ["seq=3", "fe", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["3"]);
    Ok(())
}

#[test]
fn simple_forkcat_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        &CliOptions::with_default_extensions(),
        ["str@foo=foo", "fc:", "nop", "next", "nop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}
