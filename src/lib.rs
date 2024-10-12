//! [![github]](https://github.com/cmrschwarz/scrr)&ensp;
//! [![github-build]](https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml)&ensp;
//!
//! [github]: https://img.shields.io/badge/cmrschwarz/scrr-8da0cb?&labelColor=555555&logo=github
//! [github-build]: https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml/badge.svg
//! [github-build-shields]: https://img.shields.io/github/actions/workflow/status/cmrschwarz/scrr/ci.yml?branch=main&logo=github
//!
//! A high performance pipeline processor.

use std::sync::Arc;

use once_cell::sync::Lazy;
use options::{
    context_builder::ContextBuilder, session_setup::ScrSetupOptions,
};
// we reexport the scr_core interface from this lib
pub use scr_core::*;

use extension::ExtensionRegistry;

pub static DEFAULT_EXTENSION_REGISTRY: Lazy<Arc<ExtensionRegistry>> =
    Lazy::new(build_extension_registry);

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
        .push(Box::<scr_ext_utils::UtilsExtension>::default());

    #[cfg(feature = "http")]
    extensions
        .extensions
        .push(Box::<scr_ext_http::HttpExtension>::default());

    #[cfg(feature = "python")]
    extensions
        .extensions
        .push(Box::<scr_ext_python::PythonExtension>::default());

    #[cfg(feature = "csv")]
    extensions
        .extensions
        .push(Box::<scr_ext_csv::CsvExtension>::default());

    extensions.setup();
    Arc::new(extensions)
}

pub trait ContextBuilderWithDefaultExtensions {
    fn with_default_extensions() -> Self;
}

impl ContextBuilderWithDefaultExtensions for ContextBuilder {
    fn with_default_extensions() -> Self {
        Self::with_exts(DEFAULT_EXTENSION_REGISTRY.clone())
    }
}

pub trait CliOptionsWithDefaultExtensions {
    fn with_default_extensions() -> Self;
}

impl CliOptionsWithDefaultExtensions for ScrSetupOptions {
    fn with_default_extensions() -> Self {
        Self::with_extensions(DEFAULT_EXTENSION_REGISTRY.clone())
    }
}
