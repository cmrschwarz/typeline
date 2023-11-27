pub extern crate scr_core;
use std::sync::Arc;

pub use scr_core::*;

use extension::ExtensionRegistry;

pub fn build_extension_registry() -> Arc<ExtensionRegistry> {
    #[allow(unused_mut)]
    let mut extensions = ExtensionRegistry::default();

    #[cfg(feature = "sqlite")]
    extensions
        .extensions
        .push(smallbox!(scr_ext_sqlite::SqliteExtension::default()));

    Arc::new(extensions)
}
