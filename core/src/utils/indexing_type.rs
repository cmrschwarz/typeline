use nonmax::{NonMaxU32, NonMaxUsize};

pub trait IndexingType:
    Clone
    + Copy
    + Default
    + IndexingTypeFromUsize
    + IndexingTypeIntoUsize
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
{
}
impl<I> IndexingType for I where
    I: Clone
        + Copy
        + Default
        + IndexingTypeFromUsize
        + IndexingTypeIntoUsize
        + Ord
{
}

// We can't use From/Into because this crate
// neither created the trait (e.g. From) nor the type (e.g. u32).
// We also can't provide a blanket impl for types implementing From<usize>,
// because when adding a manual implementation, we would get an error
// "upstream crate might implement From in a future version".
// Therefore we just manually implement this for all indexing types we
// want to use.
pub trait IndexingTypeIntoUsize {
    fn into_usize(self) -> usize;
}
pub trait IndexingTypeFromUsize: Sized {
    fn from_usize(v: usize) -> Self;
}

// usize
impl IndexingTypeIntoUsize for usize {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self
    }
}
impl IndexingTypeFromUsize for usize {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v
    }
}

// u32
impl IndexingTypeIntoUsize for u32 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self as usize
    }
}
impl IndexingTypeFromUsize for u32 {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as Self
    }
}

// NonMaxU32
impl IndexingTypeIntoUsize for NonMaxU32 {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self.get() as usize
    }
}
impl IndexingTypeFromUsize for NonMaxU32 {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        NonMaxU32::new(v as u32).unwrap()
    }
}

// NonMaxUsize
impl IndexingTypeIntoUsize for NonMaxUsize {
    #[inline(always)]
    fn into_usize(self) -> usize {
        self.get()
    }
}
impl IndexingTypeFromUsize for NonMaxUsize {
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        NonMaxUsize::new(v).unwrap()
    }
}
