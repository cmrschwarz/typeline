use std::ops::{Deref, DerefMut};
use std::{rc::Rc, sync::Arc};

pub trait SyncVariant: 'static {
    type Type<T: ?Sized>: Deref<Target = T>;
}
pub struct Copy;
pub struct Unique;
pub struct Local;
pub struct Shared;

#[derive(Clone, Copy)]
pub enum SyncVariantImpl {
    Unique,
    Local,
    Shared,
}

impl SyncVariant for Unique {
    type Type<T: ?Sized> = Box<T>;
}
impl SyncVariant for Local {
    type Type<T: ?Sized> = Rc<T>;
}
impl SyncVariant for Shared {
    type Type<T: ?Sized> = Arc<T>;
}
