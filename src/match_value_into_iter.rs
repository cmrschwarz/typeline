use std::mem::ManuallyDrop;

use crate::{
    match_value::{self, MatchValue, MatchValueAnySync, MatchValueFormat, MatchValueType},
    sync_variant::{self, SyncVariant, SyncVariantImpl},
};

pub struct MatchValueIntoIter<'a, ValueIter>
where
    ValueIter: Iterator<Item = MatchValue>,
{
    format: &'a MatchValueFormat,
    value_iter: ManuallyDrop<ValueIter>,
}

impl<'a, ValueIter> MatchValueIntoIter<'a, ValueIter>
where
    ValueIter: Iterator<Item = MatchValue>,
{
    pub fn new(format: &'a MatchValueFormat, value_iter: ValueIter) -> Self {
        Self {
            format,
            value_iter: ManuallyDrop::new(value_iter),
        }
    }
}

impl<'a, ValueIter> Iterator for MatchValueIntoIter<'a, ValueIter>
where
    ValueIter: Iterator<Item = MatchValue>,
{
    type Item = MatchValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.value_iter.next()
    }
}

#[inline(always)]
unsafe fn drop_typed_values<'a, T: MatchValueType>(
    repr: SyncVariantImpl,
    mut iter: impl Iterator<Item = impl MatchValueAnySync>,
) {
    match repr {
        SyncVariantImpl::Unique => loop {
            let mut next = iter.next();
            if next.is_none() {
                break;
            }
            ManuallyDrop::drop(T::storage_md_mut::<sync_variant::Unique>(
                &mut next.take().unwrap(),
            ))
        },
        SyncVariantImpl::Local => loop {
            let mut next = iter.next();
            if next.is_none() {
                break;
            }
            ManuallyDrop::drop(T::storage_md_mut::<sync_variant::Local>(
                &mut next.take().unwrap(),
            ))
        },
        SyncVariantImpl::Shared => loop {
            let mut next = iter.next();
            if next.is_none() {
                break;
            }
            ManuallyDrop::drop(T::storage_md_mut::<sync_variant::Shared>(
                &mut next.take().unwrap(),
            ))
        },
    }
}

impl<'a, ValueIter> Drop for MatchValueIntoIter<'a, ValueIter>
where
    ValueIter: Iterator<Item = MatchValue>,
{
    fn drop(&mut self) {
        use match_value::*;
        unsafe {
            let iter = ManuallyDrop::take(&mut self.value_iter);
            let repr = self.format.repr;
            match &self.format.kind {
                MatchValueKind::Text => drop_typed_values::<Text>(repr, iter),
                MatchValueKind::Bytes => drop_typed_values::<BytesType>(repr, iter),
                MatchValueKind::Error => drop_typed_values::<Error>(repr, iter),
                MatchValueKind::Html => drop_typed_values::<HtmlType>(repr, iter),
                MatchValueKind::Integer | MatchValueKind::Null => (),
                MatchValueKind::TypedArray => drop_typed_values::<TypedArray>(repr, iter),
                MatchValueKind::Array => drop_typed_values::<Array>(repr, iter),
                MatchValueKind::Object => drop_typed_values::<Object>(repr, iter),
            }
        }
    }
}
