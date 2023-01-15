use std::ops::Add;

use num::One;
use smallvec::smallvec;
use smallvec::SmallVec;

#[derive(Clone)]
pub enum RangeSpec<T: Add + Ord + One + Copy> {
    Exclude(Box<RangeSpec<T>>, Box<RangeSpec<T>>),
    Aggregate(SmallVec<[Box<RangeSpec<T>>; 2]>),
    Bounded(Option<T>, Option<T>),
    Value(T),
}
pub struct RangeIter<'a, T: Add<Output = T> + Ord + One + Copy> {
    min_bound: T,
    max_bound: T,
    positions: SmallVec<[&'a RangeSpec<T>; 2]>,
    exclude_sets: SmallVec<[&'a RangeSpec<T>; 2]>,
    offsets: SmallVec<[isize; 2]>,
}
impl<'a, T: Add<Output = T> + Ord + One + Copy> RangeIter<'a, T> {
    fn contained_in_exclude_sets(&self, v: T) -> bool {
        self.exclude_sets
            .iter()
            .any(|es| es.contains(self.min_bound, self.max_bound, v))
    }
    pub fn new(min_bound: T, max_bound: T, range_spec: &'a RangeSpec<T>) -> RangeIter<T> {
        let mut ri = RangeIter {
            min_bound,
            max_bound,
            positions: smallvec![range_spec],
            exclude_sets: SmallVec::new(),
            offsets: smallvec![-1],
        };
        ri
    }
}
impl<T: Add<Output = T> + Ord + One + Copy> RangeSpec<T> {
    pub fn contains(&self, min_bound: T, max_bound: T, v: T) -> bool {
        let mut positions: SmallVec<[&RangeSpec<T>; 4]> = smallvec![self];
        let mut offsets: SmallVec<[isize; 4]> = smallvec![-1];
        let mut exclude_sets_found_status: SmallVec<[bool; 8]> = SmallVec::new();
        loop {
            match positions.last() {
                Some(RangeSpec::Exclude(include, exclude)) => {
                    let offset = offsets.last_mut().unwrap();
                    *offset += 1;
                    //first time here, check the include set
                    if *offset == 0 {
                        exclude_sets_found_status.push(false);
                        offsets.push(-1);
                        positions.push(include);
                        continue;
                    }
                    // second time here, if the include set found it,
                    // check the exclude set
                    if *offset == 1 {
                        let status = exclude_sets_found_status.last_mut().unwrap();
                        if *status == false {
                            positions.pop();
                            offsets.pop();
                            exclude_sets_found_status.pop();
                            continue;
                        }
                        *status = false;
                        *offset = -1;
                        positions.push(exclude);
                        continue;
                    }
                    // third time here, if the exclude set found it,
                    // propagate that upwards
                    if exclude_sets_found_status.pop().unwrap() {
                        positions.pop();
                        offsets.pop();
                        exclude_sets_found_status.pop();
                        continue;
                    }
                    if let Some(esfs) = exclude_sets_found_status.last_mut() {
                        *esfs = true;
                    } else {
                        return true;
                    }
                }
                Some(RangeSpec::Aggregate(children)) => {
                    let offset = offsets.last_mut().unwrap();
                    *offset += 1;
                    if *offset as usize >= children.len()
                        || exclude_sets_found_status.last() == Some(&true)
                    {
                        positions.pop();
                        offsets.pop();
                        continue;
                    }
                    positions.push(&children[*offset as usize]);
                    offsets.push(-1);
                }
                Some(RangeSpec::Bounded(opt_min, opt_max)) => {
                    let offset = offsets.last_mut().unwrap();
                    *offset += 1;
                    let min = opt_min.unwrap_or(min_bound);
                    let max = opt_max.unwrap_or(max_bound);
                    if v < min || v >= max {
                        positions.pop();
                        offsets.pop();
                        continue;
                    }
                    if let Some(esfs) = exclude_sets_found_status.last_mut() {
                        *esfs = true;
                    } else {
                        return true;
                    }
                }
                Some(RangeSpec::Value(val)) => {
                    if *val != v {
                        positions.pop();
                        offsets.pop();
                        continue;
                    }
                    if let Some(esfs) = exclude_sets_found_status.last_mut() {
                        *esfs = true;
                    } else {
                        return true;
                    }
                }
                None => return false,
            }
        }
    }
}
impl<'a, T: Add<Output = T> + Ord + One + Copy> Iterator for RangeIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        loop {
            match self.positions.last() {
                Some(RangeSpec::Exclude(include, exclude)) => {
                    let offset = self.offsets.last_mut().unwrap();
                    if *offset != -1 {
                        self.exclude_sets.pop();
                        self.positions.pop();
                        self.offsets.pop();
                        continue;
                    }
                    *offset += 1;
                    self.exclude_sets.push(exclude);
                    self.offsets.push(-1);
                    self.positions.push(include);
                }
                Some(RangeSpec::Aggregate(children)) => {
                    let offset = self.offsets.last_mut().unwrap();
                    *offset += 1;
                    if *offset as usize >= children.len() {
                        self.positions.pop();
                        self.offsets.pop();
                        continue;
                    }
                    self.positions.push(&children[*offset as usize]);
                    self.offsets.push(-1);
                }
                Some(RangeSpec::Bounded(opt_min, opt_max)) => {
                    let offset = self.offsets.last_mut().unwrap();
                    *offset += 1;
                    let mut min = opt_min.unwrap_or(self.min_bound);
                    let max = opt_max.unwrap_or(self.max_bound);
                    // we hope that the compiler turns this loop into an add
                    for _ in 0..*offset {
                        min = min.add(T::one());
                    }
                    if min >= max {
                        self.positions.pop();
                        self.offsets.pop();
                        continue;
                    }
                    if self.contained_in_exclude_sets(min) {
                        return None;
                    }
                    return Some(min);
                }
                Some(RangeSpec::Value(v)) => {
                    self.positions.pop();
                    self.offsets.pop();
                    if *v < self.min_bound && *v >= self.max_bound {
                        return None;
                    }
                    if self.contained_in_exclude_sets(*v) {
                        return None;
                    }
                    return Some(*v);
                }
                None => return None,
            }
        }
    }
}
