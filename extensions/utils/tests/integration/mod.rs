use std::sync::Arc;

use once_cell::sync::Lazy;
use scr::extension::{Extension, ExtensionRegistry};
use scr_ext_utils::UtilsExtension;

mod basic;
mod exec;

pub static UTILS_EXTENSION_REGISTRY: Lazy<Arc<ExtensionRegistry>> =
    Lazy::new(|| {
        ExtensionRegistry::new([Box::<dyn Extension>::from(Box::new(
            UtilsExtension::default(),
        ))])
    });
