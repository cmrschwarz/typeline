//! [![github]](https://github.com/cmrschwarz/typeline/tree/main/crates/indexland)&ensp;
//! [![github-build]](https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml)&ensp;
//! [![crates-io]](https://crates.io/crates/typeline)&ensp;
//! [![msrv]](https://crates.io/crates/typeline)&ensp;
//! [![docs-rs]](https://docs.rs/typeline)&ensp;
//!
//! [github]: https://img.shields.io/badge/cmrschwarz/typeline-8da0cb?&labelColor=555555&logo=github
//! [github-build]: https://github.com/cmrschwarz/typeline/actions/workflows/ci.yml/badge.svg
//! [crates-io]: https://img.shields.io/crates/v/typeline.svg?logo=rust
//! [msrv]: https://img.shields.io/crates/msrv/typeline?logo=rust
//! [docs-rs]: https://img.shields.io/badge/docs.rs-typeline-66c2a5?logo=docs.rs
//!
//! Collections based on newtype indices for increased type safety and self
//! documenting code.
//!
//! Part of the [Typeline](https://github.com/cmrschwarz/typeline) project,
//! not ready for public use yet.
//!
//!
//! # Usage Examles
//! ```
//! use indexland::{index_newtype, IndexVec};
//! index_newtype!{
//!     struct FooIndex(u32);
//!     struct BarIndex(u32);
//! }
//! struct Foo; //...
//! struct Bar; //...
//! struct Baz{
//!     foos: IndexVec<FooIndex, Foo>,
//!     bars: IndexVec<FooIndex, Foo>,
//!     foo_offset: FooIndex,
//!     // ...
//! }
//! ```

#![warn(clippy::pedantic)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]

pub mod debuggable_nonmax;
pub mod index_slice;
pub mod index_vec;
pub mod indexing_type;
pub mod multi_ref_mut_handout;
pub mod offset_vec_deque;
pub mod phantom_slot;
pub mod random_access_container;
pub mod stable_universe;
pub mod stable_vec;
pub mod temp_vec;
pub mod universe;

pub use debuggable_nonmax::*;
pub use index_slice::*;
pub use index_vec::*;
pub use indexing_type::*;
pub use multi_ref_mut_handout::*;
pub use offset_vec_deque::*;
pub use phantom_slot::*;
pub use random_access_container::*;
pub use stable_universe::*;
pub use stable_vec::*;
pub use temp_vec::*;
pub use universe::*;

use std::ops::{Range, RangeBounds};

pub enum GetManyMutError {
    IndexOutOfBounds,
    OverlappingIndices,
}

// miscellaneous utility functions

pub fn get_two_distinct_mut<T>(
    slice: &mut [T],
    idx1: usize,
    idx2: usize,
) -> (&mut T, &mut T) {
    assert!(idx1 != idx2, "indices must be unique");
    assert!(
        idx1 < slice.len() && idx2 < slice.len(),
        "indices out of bounds"
    );
    unsafe {
        let ptr = slice.as_mut_ptr();
        (&mut *ptr.add(idx1), &mut *ptr.add(idx2))
    }
}

pub fn get_three_distinct_mut<T>(
    slice: &mut [T],
    idx1: usize,
    idx2: usize,
    idx3: usize,
) -> (&mut T, &mut T, &mut T) {
    assert!(idx1 != idx2 && idx2 != idx3, "indices must be unique");
    assert!(
        idx1 < slice.len() && idx2 < slice.len() && idx3 < slice.len(),
        "indices out of bounds"
    );
    unsafe {
        let ptr = slice.as_mut_ptr();
        (
            &mut *ptr.add(idx1),
            &mut *ptr.add(idx2),
            &mut *ptr.add(idx3),
        )
    }
}

pub fn subslice_slice_pair<'a, T>(
    slices: (&'a [T], &'a [T]),
    range: Range<usize>,
) -> (&'a [T], &'a [T]) {
    let (s1, s2) = slices;
    let s1_len = s1.len();
    if range.start > s1_len {
        (&[], &s2[range.start - s1_len..range.end - s1_len])
    } else if range.end <= s1_len {
        (&s1[range.start..range.end], &[])
    } else {
        (
            &s1[range.start..],
            &s2[..range.len() - (s1_len - range.start)],
        )
    }
}

pub fn subslice_slice_pair_mut<'a, T>(
    slices: (&'a mut [T], &'a mut [T]),
    range: Range<usize>,
) -> (&'a mut [T], &'a mut [T]) {
    let (s1, s2) = slices;
    let s1_len = s1.len();
    if range.start > s1_len {
        (&mut [], &mut s2[range.start - s1_len..range.end - s1_len])
    } else {
        (
            &mut s1[range.start..],
            &mut s2[..range.len() - (s1_len - range.start)],
        )
    }
}

pub fn range_bounds_to_range_wrapping<I: IndexingType>(
    rb: impl RangeBounds<I>,
    len: I,
) -> Range<I> {
    let start = match rb.start_bound() {
        std::ops::Bound::Included(i) => *i,
        std::ops::Bound::Excluded(i) => i.wrapping_add(I::one()),
        std::ops::Bound::Unbounded => I::ZERO,
    };
    let end = match rb.end_bound() {
        std::ops::Bound::Included(i) => i.wrapping_add(I::one()),
        std::ops::Bound::Excluded(i) => *i,
        std::ops::Bound::Unbounded => len,
    };
    start..end
}

pub fn range_bounds_to_range_usize<I: IndexingType>(
    rb: impl RangeBounds<I>,
    len: usize,
) -> Range<usize> {
    let start = match rb.start_bound() {
        std::ops::Bound::Included(i) => i.into_usize(),
        std::ops::Bound::Excluded(i) => i.into_usize() + 1,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match rb.end_bound() {
        std::ops::Bound::Included(i) => i.into_usize() + 1,
        std::ops::Bound::Excluded(i) => i.into_usize(),
        std::ops::Bound::Unbounded => len,
    };
    start..end
}

pub fn range_contains<I: PartialOrd>(
    range: Range<I>,
    subrange: Range<I>,
) -> bool {
    range.start <= subrange.start && range.end >= subrange.end
}
