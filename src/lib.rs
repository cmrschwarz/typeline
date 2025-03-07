#[doc = include_str!("../README.md")]
use std::sync::{Arc, LazyLock};

use options::{
    context_builder::ContextBuilder, session_setup::SessionSetupOptions,
};
// we reexport the typeline_core interface from this lib
pub use typeline_core::*;

use extension::ExtensionRegistry;

pub static DEFAULT_EXTENSION_REGISTRY: LazyLock<Arc<ExtensionRegistry>> =
    LazyLock::new(build_extension_registry);

pub fn build_extension_registry() -> Arc<ExtensionRegistry> {
    #[allow(unused_mut)]
    let mut extensions = ExtensionRegistry::default();

    #[cfg(feature = "sqlite")]
    extensions
        .extensions
        .push(Box::<typeline_ext_sqlite::SqliteExtension>::default());

    #[cfg(feature = "utils")]
    extensions
        .extensions
        .push(Box::<typeline_ext_utils::UtilsExtension>::default());

    #[cfg(feature = "http")]
    extensions
        .extensions
        .push(Box::<typeline_ext_http::HttpExtension>::default());

    #[cfg(feature = "python")]
    extensions
        .extensions
        .push(Box::<typeline_ext_python::PythonExtension>::default());

    #[cfg(feature = "csv")]
    extensions
        .extensions
        .push(Box::<typeline_ext_csv::CsvExtension>::default());

    #[cfg(feature = "json")]
    extensions
        .extensions
        .push(Box::<typeline_ext_json::JsonExtension>::default());

    #[cfg(feature = "selenium")]
    extensions
        .extensions
        .push(Box::<typeline_ext_selenium::SeleniumExtension>::default());

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

impl CliOptionsWithDefaultExtensions for SessionSetupOptions {
    fn with_default_extensions() -> Self {
        Self::with_extensions(DEFAULT_EXTENSION_REGISTRY.clone())
    }
}
