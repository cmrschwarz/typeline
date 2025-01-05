use typeline::{
    options::session_setup::SessionSetupOptions,
    typeline_error::ContextualizedTypelineError,
    CliOptionsWithDefaultExtensions,
};
use typeline_core::options::context_builder::ContextBuilder;

#[test]
fn parsing_macro_decl() -> Result<(), ContextualizedTypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_default_extensions(),
        ["macro:=foo", "v=[['seq', 10]]", "end", "foo", "sum"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}
