use typeline::{
    cli::{call_expr::Span, CliArgumentError},
    options::{context_builder::ContextBuilder, session_setup::SessionSetupOptions},
    typeline_error::{ContextualizedTypelineError, TypelineError},
    utils::test_utils::int_sequence_strings,
    CliOptionsWithDefaultExtensions,
};

#[test]
fn seq_sum() -> Result<(), ContextualizedTypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["seq=10", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn empty_foreach_block() -> Result<(), ContextualizedTypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["seq=10", "fe:", "end", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}

#[test]
fn foreach_block_no_colon() -> Result<(), ContextualizedTypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["seq=10", "fe", "end", "sum"],
    );
    assert_eq!(
        res.err().map(|e| e.err),
        Some(TypelineError::CliArgumentError(CliArgumentError::new(
            "unknown operator 'end'",
            Span::from_cli_arg(2, 3, 0, 3)
        )))
    );
    Ok(())
}

#[test]
fn foreach_no_block() -> Result<(), ContextualizedTypelineError> {
    // TODO: probably emit an error for this
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["seq=3", "fe", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["3"]);
    Ok(())
}

#[test]
fn simple_forkcat_block() -> Result<(), ContextualizedTypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["str@foo=foo", "fc:", "nop", "next", "nop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["foo", "foo"]);
    Ok(())
}

#[test]
fn parse_forkcat() -> Result<(), TypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["tl", "seqn=10", "forkcat:", "r=.*", "next", "drop", "end"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, int_sequence_strings(1..11));
    Ok(())
}

#[test]
fn parse_forkcat_2() -> Result<(), TypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        [
            "seq=3", "fe:", "forkcat:", "seq=2", "next", "nop", "end", "end",
        ],
    )?
    .run_collect_stringified()?;

    assert_eq!(res, ["0", "1", "0", "0", "1", "1", "0", "1", "2"]);
    Ok(())
}

#[test]
fn parse_regex_flag() -> Result<(), TypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["str=abc", "r-m=."],
    )?
    .run_collect_stringified()?;

    assert_eq!(res, ["a", "b", "c"]);
    Ok(())
}

#[test]
fn parse_setting_assignment() -> Result<(), TypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["%bs=42", "f={%bs}"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["42"]);

    Ok(())
}

#[cfg(not(miri))]
mod py {
    use rstest::rstest;
    use std::sync::Arc;
    use typeline::{
        extension::ExtensionRegistry,
        options::{
            context_builder::ContextBuilder, session_setup::SessionSetupOptions,
        },
        typeline_error::TypelineError,
    };
    use typeline_ext_python::PythonExtension;

    #[rstest]
    #[case(1, 3, "1/3")]
    #[case(1, 2, "0.5")]
    #[case(1, 7, "1/7")]
    #[case(1234, 1000, "1.234")]
    fn print_dynamic_fraction(
        #[case] num: i32,
        #[case] denom: i32,
        #[case] output: &str,
    ) -> Result<(), TypelineError> {
        let mut exts = ExtensionRegistry::default();
        exts.extensions.push(Box::new(PythonExtension::default()));
        exts.setup();
        let exts = Arc::new(exts);
        let res = ContextBuilder::from_cli_arg_strings(
            SessionSetupOptions::with_extensions(exts),
            [
                "%rpm=dynamic",
                &format!(
                    "py=import fractions; fractions.Fraction({num},{denom})"
                ),
            ],
        )?
        .run_collect_stringified()?;
        assert_eq!(res, [output]);

        Ok(())
    }
}
