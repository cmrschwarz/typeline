pub trait MaybeBoxed<T> {
    fn boxed(self) -> Box<T>;
    fn base_ref(&self) -> &T;
    fn base_ref_mut(&mut self) -> &mut T;
}

impl<T> MaybeBoxed<T> for Box<T> {
    fn boxed(self) -> Box<T> {
        todo!()
    }

    fn base_ref(&self) -> &T {
        self
    }

    fn base_ref_mut(&mut self) -> &mut T {
        &mut *self
    }
}
impl<T> MaybeBoxed<T> for T {
    fn boxed(self) -> Box<T> {
        Box::new(self)
    }

    fn base_ref(&self) -> &T {
        self
    }

    fn base_ref_mut(&mut self) -> &mut T {
        self
    }
}
