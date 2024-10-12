use std::sync::{Arc, LazyLock};

use scr::extension::{Extension, ExtensionRegistry};
use scr_ext_utils::UtilsExtension;

mod basic;
mod exec;

pub static UTILS_EXTENSION_REGISTRY: LazyLock<Arc<ExtensionRegistry>> =
    LazyLock::new(|| {
        ExtensionRegistry::new([Box::<dyn Extension>::from(Box::new(
            UtilsExtension::default(),
        ))])
    });
