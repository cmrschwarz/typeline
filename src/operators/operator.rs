use smallstr::SmallString;

use crate::{
    chain::ChainId, options::argument::CliArgIdx,
    utils::string_store::StringStoreEntry,
};

use super::{
    call::OpCall, call_concurrent::OpCallConcurrent, cast::OpCast,
    count::OpCount, file_reader::OpFileReader, fork::OpFork,
    forkcat::OpForkCat, format::OpFormat, join::OpJoin, key::OpKey,
    literal::OpLiteral, next::OpNext, print::OpPrint, regex::OpRegex,
    select::OpSelect, sequence::OpSequence, string_sink::OpStringSink,
    up::OpUp,
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

#[derive(Clone)]
pub enum OperatorData {
    Call(OpCall),
    CallConcurrent(OpCallConcurrent),
    Cast(OpCast),
    Count(OpCount),
    Print(OpPrint),
    Join(OpJoin),
    Fork(OpFork),
    ForkCat(OpForkCat),
    Next(OpNext),
    Up(OpUp),
    Key(OpKey),
    Select(OpSelect),
    Regex(OpRegex),
    Format(OpFormat),
    StringSink(OpStringSink),
    FileReader(OpFileReader),
    Literal(OpLiteral),
    Sequence(OpSequence),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
    pub append_mode: bool,
    pub transparent_mode: bool,
}

pub const DEFAULT_OP_NAME_SMALL_STR_LEN: usize = 16;

impl OperatorData {
    pub fn default_op_name(
        &self,
    ) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self {
            OperatorData::Print(_) => "p".into(),
            OperatorData::Sequence(op) => op.default_op_name(),
            OperatorData::Fork(_) => "fork".into(),
            OperatorData::ForkCat(_) => "forkcat".into(),
            OperatorData::Key(_) => "key".into(),
            OperatorData::Regex(op) => op.default_op_name(),
            OperatorData::FileReader(op) => op.default_op_name(),
            OperatorData::Format(_) => "f".into(),
            OperatorData::Select(_) => "select".into(),
            OperatorData::StringSink(op) => op.default_op_name(),
            OperatorData::Literal(op) => op.default_op_name(),
            OperatorData::Join(op) => op.default_op_name(),
            OperatorData::Next(_) => "next".into(),
            OperatorData::Up(_) => "up".into(),
            OperatorData::Count(_) => "count".into(),
            OperatorData::Cast(op) => op.default_op_name(),
            OperatorData::Call(_) => "call".into(),
            OperatorData::CallConcurrent(_) => "callcc".into(),
        }
    }
}
