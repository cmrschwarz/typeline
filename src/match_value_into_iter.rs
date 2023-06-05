use std::mem::ManuallyDrop;

use crate::match_value::{
    self, MatchValueAny, MatchValueFormat, MatchValueRepresentation, MatchValueType,
};

pub struct MatchValueIntoIter<'a, MV, ValueIter>
where
    MV: MatchValueAny,
    ValueIter: Iterator<Item = MV>,
{
    format: &'a MatchValueFormat,
    value_iter: ManuallyDrop<ValueIter>,
}

impl<'a, MV, ValueIter> MatchValueIntoIter<'a, MV, ValueIter>
where
    MV: MatchValueAny,
    ValueIter: Iterator<Item = MV>,
{
    pub fn new(format: &'a MatchValueFormat, value_iter: ValueIter) -> Self {
        Self {
            format,
            value_iter: ManuallyDrop::new(value_iter),
        }
    }
}

impl<'a, MV, ValueIter> Iterator for MatchValueIntoIter<'a, MV, ValueIter>
where
    MV: MatchValueAny,
    ValueIter: Iterator<Item = MV>,
{
    type Item = MV;
    fn next(&mut self) -> Option<Self::Item> {
        self.value_iter.next()
    }
}

#[inline(always)]
unsafe fn drop_typed_values<'a, T: MatchValueType>(
    repr: MatchValueRepresentation,
    mut iter: impl Iterator<Item = impl MatchValueAny>,
) {
    loop {
        let mut next = iter.next();
        if next.is_none() {
            break;
        }
        match repr {
            MatchValueRepresentation::Local => ManuallyDrop::drop(T::storage_local_mut(
                next.take().unwrap().as_match_value_mut(),
            )),
            MatchValueRepresentation::Shared => ManuallyDrop::drop(T::storage_shared_mut(
                next.take().unwrap().as_match_value_mut(),
            )),
        }
    }
}

impl<'a, MV, ValueIter> Drop for MatchValueIntoIter<'a, MV, ValueIter>
where
    MV: MatchValueAny,
    ValueIter: Iterator<Item = MV>,
{
    fn drop(&mut self) {
        use match_value::*;
        unsafe {
            let iter = ManuallyDrop::take(&mut self.value_iter);
            let repr = self.format.repr;
            match &self.format.kind {
                MatchValueKind::Text => drop_typed_values::<Text>(repr, iter),
                MatchValueKind::Bytes => drop_typed_values::<Bytes>(repr, iter),
                MatchValueKind::Error => drop_typed_values::<Error>(repr, iter),
                MatchValueKind::Html => drop_typed_values::<Html>(repr, iter),
                MatchValueKind::Integer | MatchValueKind::Null => (),
                MatchValueKind::Complex => unreachable!(),
                //TODO: maybe do some optimizations / recursion limitiations here
                MatchValueKind::TypedArray(_fmt) => {
                    drop_typed_values::<TypedArray<MatchValue>>(repr, iter)
                }
                MatchValueKind::Array(_fmt) => drop_typed_values::<Array<MatchValue>>(repr, iter),
                MatchValueKind::Object(_fmt) => drop_typed_values::<Object<MatchValue>>(repr, iter),
            }
        }
    }
}
