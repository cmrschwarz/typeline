use smallstr::SmallString;

use crate::{chain::ChainId, options::argument::CliArgIdx, utils::string_store::StringStoreEntry};

use super::{
    data_inserter::OpDataInserter, file_reader::OpFileReader, format::OpFormat, regex::OpRegex,
    split::OpSplit, string_sink::OpStringSink,
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub enum OperatorData {
    Print,
    Split(OpSplit),
    Regex(OpRegex),
    Format(OpFormat),
    StringSink(OpStringSink),
    FileReader(OpFileReader),
    DataInserter(OpDataInserter),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
}

pub const DEFAULT_OP_NAME_SMALL_STR_LEN: usize = 16;

impl OperatorData {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self {
            OperatorData::Print => SmallString::from("p"),
            OperatorData::Split(_) => SmallString::from("split"),
            OperatorData::Regex(re) => re.opts.default_op_name(),
            OperatorData::FileReader(fr) => fr.file_kind.default_op_name(),
            OperatorData::Format(_) => SmallString::from("f"),
            OperatorData::StringSink(_) => SmallString::from("__string_sink__"),
            OperatorData::DataInserter(di) => di.data.default_op_name(),
        }
    }
}
