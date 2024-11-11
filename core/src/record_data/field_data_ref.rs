use std::{cell::Ref, collections::VecDeque};

use crate::record_data::field_data::{
    FieldData, FieldDataBuffer, FieldValueHeader,
};

#[derive(Clone)]
pub struct DestructuredFieldDataRef<'a> {
    pub(super) headers: &'a VecDeque<FieldValueHeader>,
    pub(super) data: &'a FieldDataBuffer,
    pub(super) field_count: usize,
}

pub trait FieldDataRef: Sized {
    fn headers(&self) -> &VecDeque<FieldValueHeader>;
    fn data(&self) -> &FieldDataBuffer;
    fn field_count(&self) -> usize;
    fn equals<R: FieldDataRef>(&self, other: &R) -> bool {
        self.field_count() == other.field_count()
            && std::ptr::eq(self.headers(), other.headers())
            && std::ptr::eq(self.data(), other.data())
    }
    // We cannot derive the `Clone` trait as std::cell::Ref doesn't implement
    // it.
    fn clone_ref(&self) -> Self;
}

impl FieldDataRef for &FieldData {
    #[inline(always)]
    fn headers(&self) -> &VecDeque<FieldValueHeader> {
        &self.headers
    }
    #[inline(always)]
    fn data(&self) -> &FieldDataBuffer {
        &self.data
    }
    #[inline(always)]
    fn field_count(&self) -> usize {
        self.field_count
    }
    #[inline(always)]
    fn clone_ref(&self) -> Self {
        self
    }
}

impl<'a> FieldDataRef for Ref<'a, FieldData> {
    #[inline(always)]
    fn headers(&self) -> &VecDeque<FieldValueHeader> {
        &self.headers
    }
    #[inline(always)]
    fn data(&self) -> &FieldDataBuffer {
        &self.data
    }
    #[inline(always)]
    fn field_count(&self) -> usize {
        self.field_count
    }
    #[inline(always)]
    fn clone_ref(&self) -> Self {
        Ref::clone(self)
    }
}

impl<'a> FieldDataRef for DestructuredFieldDataRef<'a> {
    fn headers(&self) -> &VecDeque<FieldValueHeader> {
        self.headers
    }

    fn data(&self) -> &FieldDataBuffer {
        self.data
    }

    fn field_count(&self) -> usize {
        self.field_count
    }

    fn clone_ref(&self) -> Self {
        self.clone()
    }
}

impl<'a> DestructuredFieldDataRef<'a> {
    pub fn from_field_data(fd: &'a FieldData) -> Self {
        DestructuredFieldDataRef {
            headers: &fd.headers,
            data: &fd.data,
            field_count: fd.field_count,
        }
    }
}
