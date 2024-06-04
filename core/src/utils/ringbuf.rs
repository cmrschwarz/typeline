use std::{alloc::Layout, io::Write, ptr::NonNull};

// TODO: //PERF: we should probably add a `trailing_padding` member
// to this instead of calling `reserve_contiguous` all over the place

use std::ops::Range;

// this is conceptually just a VecDeque<u8>, but
// - the data buffer is at least `ALIGN` bytes aligned
// - `head` is always ALIGN bytes aligned (a `drop_front` that is not a
//   multiple of ALIGN causes a panic)
// - the physical align of any logical index will be at least as large as it's
//   logical align, modulo `ALIGN`. For example, the logical index 8 is
//   guaranteed to be at least 8 bytes aligned, assuming `ALIGN >= 8`.
// - it has less strict ownership guarantees (the data pointer is not `Unique`)
pub struct RingBuf<const ALIGN: usize> {
    data: NonNull<u8>,
    head: usize,
    len: usize,
    cap: usize,
    front_padding: u16,
    back_padding: u16,
}

const MIN_ALLOC_SIZE: usize = 64;

unsafe impl<const ALIGN: usize> Send for RingBuf<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for RingBuf<ALIGN> {}

impl<const ALIGN: usize> Default for RingBuf<ALIGN> {
    fn default() -> Self {
        // make sure align is a sane value
        Layout::from_size_align(1, ALIGN).unwrap();
        Self {
            data: NonNull::dangling(),
            head: 0,
            len: 0,
            cap: 0,
            front_padding: 0,
            back_padding: 0,
        }
    }
}

impl<const ALIGN: usize> Drop for RingBuf<ALIGN> {
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                std::alloc::dealloc(
                    self.data.as_ptr(),
                    Layout::from_size_align_unchecked(self.cap, ALIGN),
                );
            }
        }
    }
}

impl<const ALIGN: usize> Clone for RingBuf<ALIGN> {
    fn clone(&self) -> Self {
        let mut res = Self::with_capacity(self.cap);
        let (s1, s2) = self.as_slices();
        res.extend_from_slice(s1);
        res.extend_from_slice(s2);
        res
    }
}

impl<const ALIGN: usize> RingBuf<ALIGN> {
    pub const ALIGN: usize = ALIGN;
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(cap: usize) -> Self {
        let mut res = Self::default();
        unsafe { res.realloc(cap) };
        res
    }
    pub unsafe fn set_len(&mut self, len: usize) {
        self.len = len;
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        self.cap
    }
    pub fn to_physical_idx(&self, logical_idx: usize) -> usize {
        let space_back = self.space_back();
        if logical_idx >= space_back {
            return logical_idx - space_back + self.front_padding as usize;
        }
        self.head + logical_idx
    }
    pub unsafe fn realloc(&mut self, cap_new: usize) {
        let cap_old = self.cap;
        self.cap = cap_new;
        let head_old = self.head;
        self.head = 0;
        if cap_new == 0 {
            if cap_old != 0 {
                unsafe {
                    std::alloc::dealloc(
                        self.data.as_ptr(),
                        Layout::from_size_align_unchecked(cap_old, ALIGN),
                    )
                };
            }
            self.data = NonNull::dangling();
            return;
        }
        if cap_old == 0 {
            self.data = unsafe {
                NonNull::new_unchecked(std::alloc::alloc(
                    Layout::from_size_align_unchecked(cap_new, ALIGN),
                ))
            };
            return;
        }
        let data_old = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            let data_new = std::alloc::alloc(
                Layout::from_size_align_unchecked(cap_new, ALIGN),
            );
            self.data = NonNull::new_unchecked(data_new);
            std::ptr::copy_nonoverlapping(
                data_old.add(head_old),
                data_new,
                l1,
            );
            std::ptr::copy_nonoverlapping(data_old, data_new.add(l1), l2);
            std::alloc::dealloc(
                data_old,
                Layout::from_size_align_unchecked(cap_old, ALIGN),
            );
        }
    }
    fn calculate_new_cap(&self, cap_needed: usize) -> usize {
        let mut cap_new = self.cap.max(ALIGN).max(MIN_ALLOC_SIZE);
        while cap_new < cap_needed {
            cap_new <<= 1;
        }
        cap_new
    }
    pub fn reserve(&mut self, additional_cap: usize) {
        let cap_needed = self.len + additional_cap;
        if self.cap
            >= cap_needed
                + self.front_padding as usize
                + self.back_padding as usize
        {
            return;
        }
        let cap_new = self.calculate_new_cap(cap_needed);
        unsafe { self.realloc(cap_new) };
    }
    // Padding needed to ceil `len` to a multiple of `ALIGN`.
    fn len_to_align_padding(&self) -> usize {
        (ALIGN - self.len % ALIGN) % ALIGN
    }
    pub fn make_contiguous(&mut self) {
        let used_space_back = self.space_back();
        // if we don't return here, `space_back == used_space_back`
        if used_space_back >= self.len {
            return;
        }
        let ptr = self.data.as_ptr();
        let front_pad = self.front_padding as usize;
        let back_pad = self.back_padding as usize;
        self.front_padding = 0;
        self.back_padding = 0;
        let space_free = self.cap - self.len - front_pad - back_pad;
        let used_space_front = self.len - used_space_back;
        if space_free + front_pad >= used_space_back {
            // before: XDEFGH...ABCX
            // after:  ABCDEFGH.....
            unsafe {
                // X~~DEFGH.ABCX
                std::ptr::copy(
                    ptr.add(front_pad),
                    ptr.add(used_space_back),
                    used_space_front,
                );
                // ABCDEFGH.~~~X
                std::ptr::copy_nonoverlapping(
                    ptr.add(self.head),
                    ptr,
                    used_space_back,
                );
            }
            self.head = 0;
            return;
        }
        // If we want one contiguous slice at the end, we need to have this
        // amount of trailing padding to keep `head` `ALIGN`ed
        let len_ceil_pad = self.len_to_align_padding();
        if space_free + back_pad >= used_space_front + len_ceil_pad {
            let head_new =
                self.cap - used_space_back - used_space_front - len_ceil_pad;
            // before: XFGH...ABCDEX
            // after:  ....ABCDEFGHY
            unsafe {
                // XFGH..ABCDE~~X
                std::ptr::copy(
                    ptr.add(self.head),
                    ptr.add(head_new),
                    used_space_back,
                );
                // X~~~..ABCDEFGH
                std::ptr::copy_nonoverlapping(
                    ptr.add(front_pad),
                    ptr.add(self.cap - used_space_front - len_ceil_pad),
                    used_space_front,
                );
            }
            self.head = head_new;
            return;
        }

        // we have insufficient free space for a simple swap,
        // so we shove the two slices together and use `[u8]::rotate`

        // only moving front only works if it's new starting position
        // (which will become `head`) will be aligned
        let can_move_front = used_space_front % ALIGN == 0;

        // only moving the back only works if the current end of front
        // (which will become `head`) is aligned
        let can_move_back = (front_pad + used_space_front) % ALIGN == 0;

        if can_move_back
            && (used_space_back <= used_space_front || !can_move_front)
        {
            // the bottom space is smaller, and the end of front is aligned
            // so we can get away with moving bottom
            // before: XDEFGH..ABCX
            // after:  XABCDEFGH...
            unsafe {
                // XDEFGHABC~~X
                std::ptr::copy(
                    ptr.add(self.head),
                    ptr.add(front_pad + used_space_front),
                    used_space_back,
                );
                std::slice::from_raw_parts_mut(ptr.add(front_pad), self.len)
                    .rotate_left(used_space_front);
            }
            self.head = front_pad;
            return;
        }

        if can_move_front {
            let head_new = self.head - used_space_front;
            // before: XFGH.ABCDEX
            // after:  ..ABCDEFGHX
            unsafe {
                // X~FGHABCDEX
                std::ptr::copy(
                    ptr.add(front_pad),
                    ptr.add(head_new),
                    used_space_front,
                );
                // X~ABCDEFGHX
                std::slice::from_raw_parts_mut(ptr.add(head_new), self.len)
                    .rotate_left(used_space_front);
            }
            self.head = head_new;
            return;
        }

        // If neither moving front nor back is sufficient, we have to move
        // both buffers to end up with correct alignment in the end.
        // before: XFGH..ABCDEX
        // after:  ABCDEFGH....
        unsafe {
            // FGH~..ABCDEX
            std::ptr::copy(ptr.add(front_pad), ptr, used_space_front);
            // FGHABCDE~~~X
            std::ptr::copy(
                ptr.add(self.head),
                ptr.add(used_space_front),
                used_space_back,
            );
            // ABCDEFGH...X
            std::slice::from_raw_parts_mut(ptr, self.len)
                .rotate_left(used_space_front);
        }
        self.head = 0;
    }
    // `tail_data_to_join_with` is the amount of bytes currently sitting right
    // before the end of the ringbuffer that we want to also be contiguous
    // with the space reserved at the end
    pub fn reserve_contiguous(
        &mut self,
        additional_cap: usize,
        tail_data_to_join_with: usize,
    ) {
        let space_back = self.space_back();

        if self.len >= space_back {
            let free_space_front = self.head
                - (self.len - space_back)
                - self.front_padding as usize;
            if free_space_front > additional_cap
                && tail_data_to_join_with <= self.used_space_front()
            {
                return;
            }
        } else {
            // we are currently contiguous, so `tail_data_to_join_with` doesn't
            // matter
            if self.len + additional_cap <= space_back {
                return;
            }
            if self.head > additional_cap + self.len_to_align_padding() {
                return;
            }
        }
        if self.cap - self.len >= additional_cap {
            self.make_contiguous();
            return;
        }
        let cap_new = self.calculate_new_cap(self.len + additional_cap);
        unsafe { self.realloc(cap_new) };
    }
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.reserve_contiguous(slice.len(), 0);
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.tail_ptr_mut(),
                slice.len(),
            )
        };
        self.len += slice.len();
    }
    pub fn push(&mut self, v: u8) {
        self.reserve(1);
        unsafe {
            *self.tail_ptr_mut() = v;
        }
        self.len += 1;
    }
    pub fn truncate(&mut self, len: usize) {
        assert!(self.len >= len);
        let used_space_back = self.used_space_back();
        if self.len > used_space_back && len <= used_space_back {
            self.back_padding = 0;
            self.front_padding = 0;
        }
        self.len = len;
    }
    pub fn resize(&mut self, new_len: usize, value: u8) {
        if new_len > self.len {
            self.reserve(new_len - self.len);
            unsafe {
                std::ptr::write_bytes(
                    self.tail_ptr_mut(),
                    value,
                    new_len - self.len,
                )
            }
        }
        self.len = new_len;
    }
    pub fn clear(&mut self) {
        self.len = 0;
        self.head = 0;
    }
    fn space_front(&self) -> usize {
        self.head - self.front_padding as usize
    }
    fn space_back(&self) -> usize {
        self.cap - self.head - self.back_padding as usize
    }
    fn used_space_back(&self) -> usize {
        self.space_back().min(self.len)
    }
    fn used_space_front(&self) -> usize {
        self.len - self.used_space_back()
    }
    pub fn slice_lengths(&self) -> (usize, usize) {
        let len_s1 = self.space_back().min(self.len);
        let len_s2 = self.len - len_s1;
        (len_s1, len_s2)
    }
    pub fn slice_ranges(&self) -> (Range<usize>, Range<usize>) {
        let (l1, l2) = self.slice_lengths();
        (self.head..self.head + l1, 0..l2)
    }
    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        let ptr = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            (
                std::slice::from_raw_parts(ptr.add(self.head), l1),
                std::slice::from_raw_parts(ptr, l2),
            )
        }
    }
    pub fn as_slices_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        let ptr = self.data.as_ptr();
        let (l1, l2) = self.slice_lengths();
        unsafe {
            (
                std::slice::from_raw_parts_mut(ptr.add(self.head), l1),
                std::slice::from_raw_parts_mut(ptr, l2),
            )
        }
    }
    pub fn head_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.head) }
    }
    pub fn head_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.data.as_ptr().add(self.head) }
    }
    pub fn tail_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.to_physical_idx(self.len)) }
    }
    pub fn tail_ptr_mut(&mut self) -> *mut u8 {
        unsafe { self.data.as_ptr().add(self.to_physical_idx(self.len)) }
    }
    pub fn contiguous_tail_space_available(&mut self) -> usize {
        let space_back = self.space_back();
        if self.len >= space_back {
            return self.space_front() - (self.len - space_back);
        }
        space_back - self.len
    }
    pub fn range(&self, range: Range<usize>) -> RingBufIter {
        let (s1, s2) = self.as_slices();
        let l1 = s1.len();
        if range.end >= l1 {
            return RingBufIter {
                s1: s1[range].iter(),
                s2: [].iter(),
            };
        }
        if range.start >= l1 {
            return RingBufIter {
                s1: s2[range.start - l1..range.end - l1].iter(),
                s2: [].iter(),
            };
        }
        RingBufIter {
            s1: s1[range.start..range.end.min(l1)].iter(),
            s2: s2[range.start - l1..range.end - l1].iter(),
        }
    }
    pub fn ptr_from_index(&self, index: usize) -> *const u8 {
        let idx_phys = self.to_physical_idx(index);
        unsafe { self.data.as_ptr().add(idx_phys) }
    }
    pub fn ptr_from_index_mut(&mut self, index: usize) -> *mut u8 {
        let idx_phys = self.to_physical_idx(index);
        unsafe { self.data.as_ptr().add(idx_phys) }
    }
    pub fn drop_back(&mut self, count: usize) {
        assert!(count <= self.len());
        self.truncate(self.len() - count);
    }
    pub fn drop_front(&mut self, count: usize) {
        assert!(count % ALIGN == 0 && count <= self.len());
        let len_back = self.used_space_back();
        if count < len_back {
            self.head += count;
            self.len -= count;
            return;
        }
        self.head = self.front_padding as usize + count - len_back;
        self.back_padding = 0;
        self.front_padding = 0;
        self.len -= count;
    }
    pub fn buffer_range(&self) -> Range<*const u8> {
        let p = self.data.as_ptr().cast_const();
        p..unsafe { p.add(self.cap) }
    }
}

impl<const ALIGN: usize> Write for RingBuf<ALIGN> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
}

pub struct RingBufIter<'a> {
    s1: std::slice::Iter<'a, u8>,
    s2: std::slice::Iter<'a, u8>,
}

impl<'a> Iterator for RingBufIter<'a> {
    type Item = &'a u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.s1.next().or_else(|| {
            // swap to reduce branches on subsequent `next` calls
            std::mem::swap(&mut self.s1, &mut self.s2);
            self.s1.next()
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (s1_min, s1_max) = self.s1.size_hint();
        let (s2_min, s2_max) = self.s2.size_hint();
        (s1_min + s2_min, Some(s1_max.unwrap() + s2_max.unwrap()))
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.s1.count() + self.s2.count()
    }

    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.s1.last().or(self.s2.last())
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let s1_len = self.s1.size_hint().0;
        if s1_len >= n {
            return self.s1.nth(n);
        }
        self.s1 = std::mem::take(&mut self.s2);
        self.s1.nth(n - s1_len)
    }
}
