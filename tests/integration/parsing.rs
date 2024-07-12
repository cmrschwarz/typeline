use scr::{
    cli::{call_expr::Span, CliArgumentError},
    options::{
        context_builder::ContextBuilder, session_setup::ScrSetupOptions,
    },
    scr_error::{ContextualizedScrError, ScrError},
    utils::test_utils::int_sequence_strings,
    CliOptionsWithDefaultExtensions,
};

#[test]
fn seq_sum() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["seq=10", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn empty_foreach_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["seq=10", "fe:", "end", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn foreach_block_no_colon() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["seq=10", "fe", "end", "sum"],
    );
    assert_eq!(
        res.err().map(|e| e.err),
        Some(ScrError::CliArgumentError(CliArgumentError::new(
            "unknown operator 'end'",
            Span::from_cli_arg(2, 3, 0, 3)
        )))
    );
    Ok(())
}

#[test]
fn foreach_no_block() -> Result<(), ContextualizedScrError> {
    // TODO: probably emit an error for this
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["seq=3", "fe", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["3"]);
    Ok(())
}

#[test]
fn simple_forkcat_block() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["str@foo=foo", "fc:", "nop", "next", "nop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn parse_forkcat() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["scr", "seqn=10", "forkcat:", "r=.*", "next", "drop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, int_sequence_strings(1..11));
    Ok(())
}

#[test]
fn parse_forkcat_2() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        [
            "seq=3", "fe:", "forkcat:", "seq=2", "next", "nop", "end", "end",
        ],
    )?
    .run_collect_stringified()?;

    assert_eq!(res, ["0", "1", "0", "0", "1", "1", "0", "1", "2"]);
    Ok(())
}

#[test]
fn parse_regex_flag() -> Result<(), ScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["str=abc", "r-m=."],
    )?
    .run_collect_stringified()?;

    assert_eq!(res, ["a", "b", "c"]);
    Ok(())
}
