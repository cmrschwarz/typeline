//! [![github]](https://github.com/cmrschwarz/typeline)&ensp;
//! [![github-build]](https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml)&ensp;
//! [![crates-io]](https://crates.io/crates/typeline)&ensp;
//! [![msrv]](https://crates.io/crates/typeline)&ensp;
//! [![docs-rs]](https://docs.rs/typeline)&ensp;
//!
//! [github]: https://img.shields.io/badge/cmrschwarz/typeline-8da0cb?&labelColor=555555&logo=github
//! [github-build]: https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml/badge.svg
//! [crates-io]: https://img.shields.io/crates/v/typeline.svg?logo=rust
//! [msrv]: https://img.shields.io/crates/msrv/typeline?logo=rust
//! [docs-rs]: https://img.shields.io/badge/docs.rs-typeline-66c2a5?logo=docs.rs
//!
//! An efficient, type-safe pipeline processing system.
//!
//!
//! # Usage Examles
//!
//! ## Add Leading Zeroes to Numbered Files
//!
//! ```bash
//! tl ls r="foo_(?<id>\d+)\.txt" mv="foo_{id:02}.txt"
//! ```
//!
//! ## Advent of Code (Day 1, Part 1, 2023)
//! ```bash
//! tl <input.txt lines fe: r-m='\d' fc: head next tail end join to_int end sum
//! ```
//!
//! ## Download all PNG Images from a Website
//! ```bash
//! tl str="https://google.com" GET xpath="//@href" r-f="\.png$" GET enum-n write="{:02}.png"
//! ```

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
