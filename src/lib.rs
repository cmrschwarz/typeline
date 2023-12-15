pub extern crate scr_core;
use std::sync::Arc;

// we reexport the scr_core interface from this lib
pub use scr_core::*;

use extension::ExtensionRegistry;

pub fn build_extension_registry() -> Arc<ExtensionRegistry> {
    #[allow(unused_mut)]
    let mut extensions = ExtensionRegistry::default();

    #[cfg(feature = "sqlite")]
    extensions
        .extensions
        .push(Box::<scr_ext_sqlite::SqliteExtension>::default());

    #[cfg(feature = "misc_cmds")]
    extensions
        .extensions
        .push(Box::<scr_ext_misc_cmds::MiscCmdsExtension>::default());

    #[cfg(feature = "http")]
    extensions
        .extensions
        .push(Box::<scr_ext_http::HttpExtension>::default());

    extensions.setup();
    Arc::new(extensions)
}
