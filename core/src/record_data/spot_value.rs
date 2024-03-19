use arrayvec::{ArrayString, ArrayVec};

use super::stream_value::StreamValueId;

type SpotReferenceId = usize;

pub enum SpotValue {
    Undefined,
    Null,
    Int(i64),
    Float(f64),
    SmallBytes(ArrayString<24>),
    SmallText(ArrayVec<u8, 24>),
    StreamValue(StreamValueId),
    SpotReference(SpotReferenceId),
}
