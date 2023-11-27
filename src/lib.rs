pub extern crate scr_core;
pub use scr_core::*;

use extension::ExtensionRegistry;

pub fn build_extension_registry() -> ExtensionRegistry {
    #[allow(unused_mut)]
    let mut extensions = ExtensionRegistry::default();

    #[cfg(feature = "sqlite")]
    extensions
        .extensions
        .push(smallbox!(scr_ext_sqlite::SqliteExtension::default()));

    extensions
}
