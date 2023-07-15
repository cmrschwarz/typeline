use smallstr::SmallString;

use crate::{chain::ChainId, options::argument::CliArgIdx, utils::string_store::StringStoreEntry};

use super::{
    chain_navigation_ops::{OpNext, OpUp},
    data_inserter::OpDataInserter,
    file_reader::OpFileReader,
    format::OpFormat,
    key::OpKey,
    print::OpPrint,
    regex::OpRegex,
    select::OpSelect,
    sequence::OpSequence,
    split::OpSplit,
    string_sink::OpStringSink,
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

#[derive(Clone)]
pub enum OperatorData {
    Print(OpPrint),
    Split(OpSplit),
    Next(OpNext),
    Up(OpUp),
    Key(OpKey),
    Select(OpSelect),
    Regex(OpRegex),
    Format(OpFormat),
    StringSink(OpStringSink),
    FileReader(OpFileReader),
    DataInserter(OpDataInserter),
    Sequence(OpSequence),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
    pub append_mode: bool,
}

pub const DEFAULT_OP_NAME_SMALL_STR_LEN: usize = 16;

impl OperatorData {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self {
            OperatorData::Print(_) => SmallString::from("p"),
            OperatorData::Sequence(op) => op.default_op_name(),
            OperatorData::Split(_) => SmallString::from("split"),
            OperatorData::Key(_) => SmallString::from("key"),
            OperatorData::Regex(op) => op.default_op_name(),
            OperatorData::FileReader(op) => op.default_op_name(),
            OperatorData::Format(_) => SmallString::from("f"),
            OperatorData::Select(_) => SmallString::from("select"),
            OperatorData::StringSink(op) => op.default_op_name(),
            OperatorData::DataInserter(op) => op.default_op_name(),
            OperatorData::Next(_) => SmallString::from("next"),
            OperatorData::Up(_) => SmallString::from("up"),
        }
    }
}
