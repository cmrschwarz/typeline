use std::{collections::HashMap, fmt::Write};

use smallstr::SmallString;

use crate::{
    chain::{Chain, ChainId},
    context::{Session, SessionSettings},
    job_session::JobData,
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
    },
    options::{argument::CliArgIdx, session_options::SessionOptions},
    record_data::field::FieldId,
    utils::{
        identity_hasher::BuildIdentityHasher,
        small_box::SmallBox,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::{
    aggregator::{OpAggregator, AGGREGATOR_DEFAULT_NAME},
    call::OpCall,
    call_concurrent::OpCallConcurrent,
    cast::OpCast,
    count::OpCount,
    end::OpEnd,
    errors::OperatorSetupError,
    explode::OpExplode,
    field_value_sink::OpFieldValueSink,
    file_reader::OpFileReader,
    fork::OpFork,
    forkcat::OpForkCat,
    format::OpFormat,
    join::OpJoin,
    key::OpKey,
    literal::OpLiteral,
    next::OpNext,
    nop::OpNop,
    nop_copy::OpNopCopy,
    print::OpPrint,
    regex::OpRegex,
    select::OpSelect,
    sequence::OpSequence,
    string_sink::OpStringSink,
    transform::{TransformData, TransformState},
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub enum OperatorData {
    Nop(OpNop),
    NopCopy(OpNopCopy),
    Call(OpCall),
    CallConcurrent(OpCallConcurrent),
    Cast(OpCast),
    Count(OpCount),
    Print(OpPrint),
    Join(OpJoin),
    Fork(OpFork),
    ForkCat(OpForkCat),
    Next(OpNext),
    End(OpEnd),
    Key(OpKey),
    Select(OpSelect),
    Regex(OpRegex),
    Format(OpFormat),
    StringSink(OpStringSink),
    FieldValueSink(OpFieldValueSink),
    FileReader(OpFileReader),
    Literal(OpLiteral),
    Sequence(OpSequence),
    Explode(OpExplode),
    Aggregator(OpAggregator),
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
            OperatorData::FieldValueSink(op) => op.default_op_name(),
            OperatorData::Literal(op) => op.default_op_name(),
            OperatorData::Join(op) => op.default_op_name(),
            OperatorData::Next(_) => "next".into(),
            OperatorData::End(_) => "end".into(),
            OperatorData::Count(_) => "count".into(),
            OperatorData::Cast(op) => op.default_op_name(),
            OperatorData::Call(_) => "call".into(),
            OperatorData::CallConcurrent(_) => "callcc".into(),
            OperatorData::Nop(_) => "nop".into(),
            OperatorData::NopCopy(_) => "nop-c".into(),
            OperatorData::Explode(op) => op.default_name(),
            OperatorData::Custom(op) => op.default_name(),
            OperatorData::Aggregator(_) => AGGREGATOR_DEFAULT_NAME.into(),
        }
    }
    pub fn debug_op_name(&self) -> DefaultOperatorName {
        match self {
            OperatorData::Aggregator(op) => {
                let mut n = self.default_op_name();
                n.push('<');
                for (i, &so) in op.sub_ops.iter().enumerate() {
                    if i > 0 {
                        n.push_str(", ");
                    }
                    n.write_fmt(format_args!("{so}")).unwrap();
                }
                n.push('>');
                n
            }
            _ => self.default_op_name(),
        }
    }
    pub fn can_be_appended(&self) -> bool {
        match self {
            OperatorData::Print(_) => true,
            OperatorData::Sequence(_) => true,
            OperatorData::Fork(_) => false,
            OperatorData::ForkCat(_) => false,
            OperatorData::Key(_) => false,
            OperatorData::Regex(_) => true,
            OperatorData::FileReader(_) => true,
            OperatorData::Format(_) => true,
            OperatorData::Select(_) => false,
            OperatorData::StringSink(_) => true,
            OperatorData::FieldValueSink(_) => true,
            OperatorData::Literal(_) => true,
            OperatorData::Join(_) => true,
            OperatorData::Next(_) => false,
            OperatorData::End(_) => false,
            OperatorData::Count(_) => true,
            OperatorData::Cast(_) => true,
            OperatorData::Call(_) => true,
            OperatorData::CallConcurrent(_) => true,
            OperatorData::Nop(_) => true,
            OperatorData::NopCopy(_) => true,
            OperatorData::Explode(_) => true,
            OperatorData::Custom(op) => op.can_be_appended(),
            OperatorData::Aggregator(_) => true,
        }
    }
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> DefaultOperatorName;

    // mainly used for operators that start subchains
    // makes sure that e.g. `scr seqn=10 fork +int=11 p`
    // does not try to aggregate `fork` with `int`
    // `fork` cannot be appended directly (although `fork end +int=42` is legal)
    fn can_be_appended(&self) -> bool {
        false
    }
    fn output_count(&self, _op_base: &OperatorBase) -> usize;
    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool;
    fn on_op_added(&self, _sess: &mut SessionOptions) {}
    fn on_subchains_added(&mut self, _current_subchain_count: u32) {}
    fn register_output_var_names(
        &self,
        _ld: &mut LivenessData,
        _sess: &Session,
    ) {
    }

    // all of the &mut bool flags default to true
    // turning them to false allows for some pipeline optimizations
    // but may cause incorrect behavior if the promises made are broken later
    fn update_variable_liveness(
        &self,
        _ld: &mut LivenessData,
        _bb_id: BasicBlockId,
        access_flags: &mut AccessFlags,
    );
    fn setup(
        &mut self,
        _op_id: OperatorId,
        _op_base: &OperatorBase,
        _chain: &Chain,
        _setttings: &SessionSettings,
        _chain_labels: &HashMap<
            StringStoreEntry,
            ChainId,
            BuildIdentityHasher,
        >,
        _ss: &mut StringStore,
    ) -> Result<(), OperatorSetupError> {
        Ok(())
    }
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
