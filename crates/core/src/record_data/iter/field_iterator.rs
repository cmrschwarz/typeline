use std::cmp::Ordering;

use bitbybit::bitfield;

use crate::record_data::{
    field::{FieldId, FieldManager},
    field_data::{
        FieldValueFormat, FieldValueHeader, FieldValueRepr, RunLength,
    },
    field_value_ref::{TypedField, ValidTypedRange},
    iter_hall::{FieldLocation, IterLocation},
};

use super::{
    super::field_data_ref::FieldDataRef, field_iter::FieldIter,
    iter_adapters::BoundedIter, ref_iter::AutoDerefIter,
};

#[bitfield(u8, default = 0b00000)]
pub struct FieldIterScanOptions {
    #[bit(0, rw)]
    stop_on_dead: bool,
    #[bit(1, rw)]
    allow_different_kind_if_dead: bool,
    #[bit(2, rw)]
    allow_header_ring_wrap: bool,
    #[bit(3, rw)]
    allow_data_ring_wrap: bool,
    #[bit(4, rw)]
    invert_kinds_check: bool,
}

#[bitfield(u8, default = 0b0)]
pub struct FieldIterRangeOptions {
    #[bit(0, rw)]
    allow_pointing_at_dead: bool,
}

// While this type does not implement the `std::iter::Iterator` trait directly,
// it is crutial that it has the same basic property:
// References handed out by it have a constant lifetime 'a that is independant
// of the lifetime of the iterator itself.
// This is necessary because `AutoDerefIterator` will temporarily construct
// iterators to referenced fields but need the lifetimes to elements returned
// by these temporary
pub trait FieldIterator: Sized + Clone {
    type FieldDataRefType: FieldDataRef;
    fn field_data_ref(&self) -> &Self::FieldDataRefType;
    fn into_base_iter(self) -> FieldIter<Self::FieldDataRefType>;
    fn get_next_field_pos(&self) -> usize;
    fn is_next_valid(&self) -> bool;
    fn is_prev_valid(&self) -> bool;
    fn get_next_field_format(&self) -> FieldValueFormat;
    fn get_next_field_data(&self) -> usize;
    fn get_prev_field_data_end(&self) -> usize;
    // if the cursor is in the middle of a header, *that* header will be
    // returned, not the one after it
    fn get_next_header(&self) -> Option<FieldValueHeader>;
    fn get_next_field_header_data_start(&self) -> usize;
    fn get_next_field_header_ref(&self) -> &FieldValueHeader {
        &self.field_data_ref().headers()[self.get_next_header_index()]
    }
    fn get_next_header_index(&self) -> usize;
    fn get_prev_header_index(&self) -> usize;
    fn get_next_typed_field(&mut self) -> TypedField;
    fn field_run_length_fwd(&self) -> RunLength;
    fn field_run_length_bwd(&self) -> RunLength;
    fn is_next_valid_alive(&self) -> bool {
        self.get_next_header().map(|h| !h.deleted()).unwrap_or(true)
    }
    fn field_run_length_fwd_oversize(&self) -> RunLength {
        if self.field_run_length_bwd() != 0 {
            return self.field_run_length_fwd();
        }
        0
    }
    fn get_field_location_after_last(&self) -> FieldLocation {
        FieldLocation {
            field_pos: self.get_next_field_pos(),
            header_idx: self.get_next_header_index(),
            header_rl_offset: self.field_run_length_bwd(),
            data_pos: self.get_prev_field_data_end(),
        }
    }
    fn get_field_location_before_next(&self) -> FieldLocation {
        FieldLocation {
            field_pos: self.get_next_field_pos(),
            header_idx: self.get_next_header_index(),
            header_rl_offset: self.field_run_length_bwd(),
            data_pos: self.get_next_field_data(),
        }
    }
    fn get_iter_location(&self) -> IterLocation {
        IterLocation {
            field_pos: self.get_next_field_pos(),
            header_idx: self.get_next_header_index(),
            header_rl_offset: self.field_run_length_bwd(),
            header_start_data_pos_post_padding: self
                .get_next_field_header_data_start(),
        }
    }
    fn next_header(&mut self, skip_deleted: bool) -> RunLength;
    fn prev_header(&mut self, skip_deleted: bool) -> RunLength;
    fn next_field(&mut self) -> RunLength;
    fn prev_field(&mut self) -> RunLength;
    fn next_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterScanOptions,
    ) -> usize;
    fn prev_n_fields_with_fmt<const N: usize>(
        &mut self,
        n: usize,
        kinds: [FieldValueRepr; N],
        opts: FieldIterScanOptions,
    ) -> usize;
    fn move_to_field_pos(&mut self, field_pos: usize) {
        let curr = self.get_next_field_pos();
        match curr.cmp(&field_pos) {
            Ordering::Equal => (),
            Ordering::Less => {
                let diff = field_pos - curr;
                let adv = self.next_n_fields(diff, true);
                debug_assert_eq!(diff, adv);
            }
            Ordering::Greater => {
                let diff = curr - field_pos;
                let adv = self.prev_n_fields(diff, true);
                debug_assert_eq!(diff, adv);
            }
        }
    }
    fn next_n_fields(&mut self, n: usize, allow_ring_wrap: bool) -> usize {
        self.next_n_fields_with_fmt(
            n,
            [],
            FieldIterScanOptions::default()
                .with_allow_header_ring_wrap(allow_ring_wrap)
                .with_allow_data_ring_wrap(allow_ring_wrap)
                .with_invert_kinds_check(true),
        )
    }
    fn prev_n_fields(&mut self, n: usize, allow_ring_wrap: bool) -> usize {
        self.prev_n_fields_with_fmt(
            n,
            [],
            FieldIterScanOptions::default()
                .with_allow_header_ring_wrap(allow_ring_wrap)
                .with_allow_data_ring_wrap(allow_ring_wrap)
                .with_invert_kinds_check(true),
        )
    }
    fn move_n_fields(&mut self, delta: isize, allow_ring_wrap: bool) -> isize {
        if delta < 0 {
            -(self.prev_n_fields((-delta) as usize, allow_ring_wrap) as isize)
        } else {
            self.next_n_fields(delta as usize, allow_ring_wrap) as isize
        }
    }
    fn typed_field_fwd(&mut self, limit: usize) -> Option<TypedField>;
    fn typed_field_bwd(&mut self, limit: usize) -> Option<TypedField>;
    fn typed_range_fwd(
        &mut self,
        limit: usize,
        opts: FieldIterRangeOptions,
    ) -> Option<ValidTypedRange>;
    fn typed_range_bwd(
        &mut self,
        limit: usize,
        opts: FieldIterRangeOptions,
    ) -> Option<ValidTypedRange>;
    fn bounded(self, backwards: usize, forwards: usize) -> BoundedIter<Self> {
        BoundedIter::new_relative(self, backwards, forwards)
    }
    fn auto_deref(
        self,
        fm: &FieldManager,
        field_id: FieldId,
    ) -> AutoDerefIter<Self> {
        AutoDerefIter::new(fm, field_id, self)
    }
}
