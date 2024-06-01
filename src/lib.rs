pub extern crate scr_core;

extern crate lazy_static;

use std::sync::Arc;

use options::context_builder::ContextBuilder;
// we reexport the scr_core interface from this lib
pub use scr_core::*;

use extension::ExtensionRegistry;
use scr_core::{
    cli::{parse_cli, CliOptions},
    options::session_options::SessionOptions,
    scr_error::ContextualizedScrError,
};

lazy_static::lazy_static! {
    pub static ref DEFAULT_EXTENSION_REGISTRY: Arc<ExtensionRegistry> = build_extension_registry();
}

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

    #[cfg(feature = "python")]
    extensions
        .extensions
        .push(Box::<scr_ext_python::PythonExtension>::default());

    extensions.setup();
    Arc::new(extensions)
}

pub trait ContextBuilderWithDefaultExts {
    fn with_default_exts() -> Self;
}

impl ContextBuilderWithDefaultExts for ContextBuilder {
    fn with_default_exts() -> Self {
        Self::from_extensions(DEFAULT_EXTENSION_REGISTRY.clone())
    }
}

pub fn parse_cli_from_strings<'a>(
    cli_opts: CliOptions,
    args: impl IntoIterator<Item = impl Into<&'a str>>,
) -> Result<SessionOptions, ContextualizedScrError> {
    parse_cli(
        args.into_iter()
            .map(|v| v.into().as_bytes().to_vec())
            .collect(),
        cli_opts,
        build_extension_registry(),
    )
}
