use std::collections::HashMap;

use bstring::BString;

use crate::{
    chain::ChainId,
    worker_thread_session::{FieldId, WorkerThreadSession},
};

use super::operator_base::OperatorOffsetInChain;

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
    _sess: &'_ mut WorkerThreadSession<'a>,
    output_field: FieldId,
    op: &'b OpFormat,
) -> TfFormat<'b> {
    //TODO: cache field indices...
    TfFormat {
        output_field,
        parts: &op.parts,
    }
}
