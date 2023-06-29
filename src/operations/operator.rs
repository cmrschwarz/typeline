use smallstr::SmallString;

use crate::{chain::ChainId, options::argument::CliArgIdx, utils::string_store::StringStoreEntry};

use super::{format::OpFormat, regex::OpRegex, split::OpSplit, string_sink::OpStringSink};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub enum OperatorData {
    Print,
    Split(OpSplit),
    Regex(OpRegex),
    Format(OpFormat),
    StringSink(OpStringSink),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
}

impl OperatorData {
    pub fn default_op_name(&self) -> SmallString<[u8; 16]> {
        match self {
            OperatorData::Print => SmallString::from("p"),
            OperatorData::Split(_) => SmallString::from("split"),
            OperatorData::Regex(re) => re.opts.default_op_name(),
            OperatorData::Format(_) => SmallString::from("f"),
            OperatorData::StringSink(_) => SmallString::from("__string_sink__"),
        }
    }
}
