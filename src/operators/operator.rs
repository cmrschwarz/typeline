use std::collections::HashMap;

use smallstr::SmallString;

use crate::{
    chain::{Chain, ChainId},
    context::{Session, SessionSettings},
    job_session::JobData,
    liveness_analysis::{BasicBlockId, LivenessData, OpOutputIdx},
    options::{argument::CliArgIdx, session_options::SessionOptions},
    record_data::field::FieldId,
    utils::{
        identity_hasher::BuildIdentityHasher,
        small_box::SmallBox,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    call::OpCall,
    call_concurrent::OpCallConcurrent,
    cast::OpCast,
    count::OpCount,
    errors::OperatorSetupError,
    file_reader::OpFileReader,
    fork::OpFork,
    forkcat::OpForkCat,
    format::OpFormat,
    join::OpJoin,
    key::OpKey,
    literal::OpLiteral,
    next::OpNext,
    nop::OpNop,
    print::OpPrint,
    regex::OpRegex,
    select::OpSelect,
    sequence::OpSequence,
    string_sink::OpStringSink,
    transform::{TransformData, TransformState},
    up::OpUp,
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub enum OperatorData {
    Nop(OpNop),
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
    Custom(SmallBox<dyn Operator, 96>),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: Option<ChainId>,
    pub offset_in_chain: OperatorOffsetInChain,
    pub append_mode: bool,
    pub transparent_mode: bool,
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
    pub desired_batch_size: usize,
}

pub type DefaultOperatorName = SmallString<[u8; 16]>;

impl OperatorData {
    pub fn default_op_name(&self) -> DefaultOperatorName {
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
            OperatorData::Nop(_) => "nop".into(),
            OperatorData::Custom(op) => op.default_name(),
        }
    }
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> DefaultOperatorName;
    fn logical_output_count(&self, op_base: &OperatorBase) -> usize;
    fn output_count(&self, op_base: &OperatorBase) -> usize {
        let mut oc = self.logical_output_count(op_base);
        if op_base.append_mode {
            oc = oc.wrapping_sub(1)
        }
        oc
    }
    fn on_op_added(&self, _sess: &mut SessionOptions) {}
    fn on_subchains_added(&mut self, _current_subchain_count: u32) {}
    fn register_output_var_names(&self, ld: &mut LivenessData, sess: &Session);
    fn update_variable_liveness(
        &self,
        ld: &mut LivenessData,
        bb_id: BasicBlockId,
        bb_offset: u32,
        input_accessed: &mut bool,
        input_access_stringified: &mut bool,
        may_dup_or_drop: &mut bool,
    );
    fn setup(
        &mut self,
        op_id: OperatorId,
        op_base: &OperatorBase,
        chain: &Chain,
        setttings: &SessionSettings,
        chain_labels: &HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
        ss: &mut StringStore,
    ) -> Result<(), OperatorSetupError>;
    fn on_liveness_computed(
        &mut self,
        _sess: &Session,
        _op_id: OperatorId,
        _ld: &LivenessData,
    ) {
    }
    fn build_transform<'a>(
        &'a self,
        sess: &mut JobData,
        op_base: &OperatorBase,
        tf_state: &mut TransformState,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a>;
}
