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
        .push(Box::<scr_ext_sqlite::SqliteExtension>::default());

    #[cfg(feature = "misc_cmds")]
    extensions
        .extensions
        .push(Box::<scr_ext_misc_cmds::MiscCmdsExtension>::default());

    extensions.setup();
    Arc::new(extensions)
}
