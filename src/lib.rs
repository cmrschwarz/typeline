pub extern crate scr_core;
use std::sync::Arc;

// we reexport the scr_core interface from this lib
pub use scr_core::*;

use extension::ExtensionRegistry;
use scr_core::{
    cli::parse_cli, options::session_options::SessionOptions,
    scr_error::ContextualizedScrError,
};

pub fn build_extension_registry() -> Arc<ExtensionRegistry> {
    #[allow(unused_mut)]
    let mut extensions = ExtensionRegistry::default();

    #[cfg(feature = "sqlite")]
    extensions
        .extensions
        .push(Box::<scr_ext_sqlite::SqliteExtension>::default());

    #[cfg(feature = "utils")]
    extensions
        .extensions
        .push(Box::<scr_ext_utils::MiscCmdsExtension>::default());

    #[cfg(feature = "http")]
    extensions
        .extensions
        .push(Box::<scr_ext_http::HttpExtension>::default());

    extensions.setup();
    Arc::new(extensions)
}

pub fn parse_cli_from_strings<'a>(
    args: impl IntoIterator<Item = impl Into<&'a str>>,
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli(
        args.into_iter()
            .map(|v| v.into().as_bytes().to_vec())
            .collect(),
        cfg!(feature = "repl"),
        build_extension_registry(),
    )
}
