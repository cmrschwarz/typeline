use smallvec::SmallVec;

use crate::chain::ChainId;

use super::range_spec::RangeSpec;

#[derive(Clone)]
pub enum ChainSpec {
    ApplyMult(SmallVec<[Box<ChainSpec>; 2]>),
    Parallel(SmallVec<[Box<ChainSpec>; 2]>),
    Parent(usize),
    Absolute(ChainId),
    Children(RangeSpec<ChainId>),
}
