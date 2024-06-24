//! [![github]](https://github.com/cmrschwarz/scrr)&ensp;
//! [![github-build]](https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml)&ensp;
//!
//! [github]: https://img.shields.io/badge/cmrschwarz/scrr-8da0cb?&labelColor=555555&logo=github
//! [github-build]: https://github.com/cmrschwarz/scrr/actions/workflows/ci.yml/badge.svg
//! [github-build-shields]: https://img.shields.io/github/actions/workflow/status/cmrschwarz/scrr/ci.yml?branch=main&logo=github
//!
//! A high performance pipeline processor.

pub extern crate scr_core;

use std::sync::Arc;

use cli::CliOptions;
use once_cell::sync::Lazy;
use options::context_builder::ContextBuilder;
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

pub trait CliOptionsWithDefaultExts {
    fn with_default_exts() -> Self;
}

impl CliOptionsWithDefaultExts for CliOptions {
    fn with_default_exts() -> Self {
        Self::with_extensions(DEFAULT_EXTENSION_REGISTRY.clone())
    }
}
