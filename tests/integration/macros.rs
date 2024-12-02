use scr::{
    options::session_setup::ScrSetupOptions,
    scr_error::ContextualizedScrError, CliOptionsWithDefaultExtensions,
};
use scr_core::options::context_builder::ContextBuilder;

#[test]
fn parsing_macro_decl() -> Result<(), ContextualizedScrError> {
    let res = ContextBuilder::from_cli_arg_strings(
        ScrSetupOptions::with_default_extensions(),
        ["macro:=foo", "v=[['seq', 10]]", "end", "foo"],
    )?
    .run_collect_stringified()?;
    assert_eq!(res, ["45"]);
    Ok(())
}
