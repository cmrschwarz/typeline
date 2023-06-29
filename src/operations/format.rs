use std::collections::HashMap;

use bstr::BString;

use crate::{
    chain::ChainId,
    worker_thread_session::{FieldId, JobData, MatchSetId},
};

use super::{operator::OperatorOffsetInChain, transform::TransformData};

enum FormatKeyLocationReference {
    Index(u32),
    Name(String), //TODO: string interning
}

enum FormatKeyFormat {
    Plain,
    ZeroPaddedInt(u32),
    NDecimalFloat(u32),
    ZeroPaddedNDecimalFloat(u32, u32),
}

pub struct FormatKey {
    location_ref: FormatKeyLocationReference,
    chain_offsets: HashMap<ChainId, Option<OperatorOffsetInChain>>,
    format: FormatKeyFormat,
}

pub enum FormatPart {
    //TODO: string interning
    ByteLiteral(BString),
    TextLiteral(String),
    Key(FormatKey),
}

pub struct OpFormat {
    pub parts: Vec<FormatPart>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a [FormatPart],
}

pub fn setup_tf_format<'a, 'b>(
    sess: &'_ mut JobData<'a>,
    match_set_id: MatchSetId,
    _input_field: FieldId,
    op: &'b OpFormat,
) -> (TransformData<'b>, FieldId) {
    //TODO: cache field indices...
    let output_field = sess.entry_data.add_field(match_set_id, None);
    let tf = TfFormat {
        output_field,
        parts: &op.parts,
    };
    (TransformData::Format(tf), output_field)
}
