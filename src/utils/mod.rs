pub mod plattform;
pub mod scratch_vec;
pub mod string_store;
pub mod universe;

pub fn is_increasing<T: PartialOrd>(mut iter: impl Iterator<Item = T>) -> bool {
    if let Some(first) = iter.next() {
        let prev = first;
        for n in iter {
            if prev > n {
                return false;
            }
        }
    }
    true
}
pub fn is_strictly_increasing<T: PartialOrd>(mut iter: impl Iterator<Item = T>) -> bool {
    if let Some(first) = iter.next() {
        let prev = first;
        for n in iter {
            if prev >= n {
                return false;
            }
        }
    }
    true
}
pub fn vec_insert_n<T>(
    vec: &mut Vec<T>,
    insert_points: &[usize],
    mut data: impl Iterator<Item = T>,
) {
    let n = insert_points.len();
    if n == 0 {
        return;
    }
    if n == 1 {
        vec.insert(insert_points[0], data.next().unwrap());
        return;
    }
    assert!(is_strictly_increasing(insert_points.iter()));
    vec.reserve(n);
    unsafe {
        let first = *insert_points.first().unwrap_unchecked();
        let last = *insert_points.last().unwrap_unchecked();
        let head = vec.as_mut_ptr();
        let insertion_begin = head.add(first);
        let insertion_end = head.add(last);
        std::ptr::copy(insertion_end, insertion_end.add(n), n);
        let mut i = insert_points.len() - 2;
        let mut gap_end = last;
        let mut gap_start = last - 1;
        while insert_points[i] == gap_start && i > 0 {
            i -= 1;
        }
    }
}
