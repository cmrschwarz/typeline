use std::{collections::HashMap, fmt::Write};

use smallstr::SmallString;

use crate::{
    chain::{Chain, ChainId},
    context::{SessionData, SessionSettings},
    job::{add_transform_to_job, Job, JobData},
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
    aggregator::{
        insert_tf_aggregator, OpAggregator, AGGREGATOR_DEFAULT_NAME,
    },
    call::{build_tf_call, OpCall},
    call_concurrent::{build_tf_call_concurrent, OpCallConcurrent},
    count::{build_tf_count, OpCount},
    end::OpEnd,
    errors::OperatorSetupError,
    field_value_sink::{build_tf_field_value_sink, OpFieldValueSink},
    file_reader::{build_tf_file_reader, OpFileReader},
    foreach::{insert_tf_foreach, OpForeach},
    fork::{build_tf_fork, OpFork},
    forkcat::{insert_tf_forkcat, OpForkCat},
    format::{build_tf_format, OpFormat},
    join::{build_tf_join, OpJoin},
    key::OpKey,
    literal::{build_tf_literal, OpLiteral},
    next::OpNext,
    nop::{build_tf_nop, OpNop},
    nop_copy::{build_tf_nop_copy, OpNopCopy},
    print::{build_tf_print, OpPrint},
    regex::{build_tf_regex, OpRegex},
    select::{build_tf_select, OpSelect},
    sequence::{build_tf_sequence, OpSequence},
    string_sink::{build_tf_string_sink, OpStringSink},
    to_str::{build_tf_to_str, OpToStr},
    transform::{TransformData, TransformId, TransformState},
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub enum OperatorData {
    Nop(OpNop),
    NopCopy(OpNopCopy),
    Call(OpCall),
    CallConcurrent(OpCallConcurrent),
    ToStr(OpToStr),
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
    Aggregator(OpAggregator),
    Foreach(OpForeach),
    Custom(SmallBox<dyn Operator, 96>),
}

pub struct OperatorBase {
    pub argname: StringStoreEntry,
    pub label: Option<StringStoreEntry>,
    pub cli_arg_idx: Option<CliArgIdx>,
    pub chain_id: Option<ChainId>,
    pub offset_in_chain: OperatorOffsetInChain,
    pub transparent_mode: bool,
    pub desired_batch_size: usize,
    // this is not part of the OperatorLivenessData struct because it is
    // used in the `prebound_outputs` mechanism that is used during
    // operators -> transforms expansion long after liveness analysis
    // has concluded
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TransformContinuationKind {
    Regular,
    SelfExpanded,
}

pub struct OperatorInstantiation {
    pub tfs_begin: TransformId,
    pub tfs_end: TransformId,
    pub next_input_field: FieldId,
    pub continuation: TransformContinuationKind,
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub enum OutputFieldKind {
    #[default]
    Unique,
    Dummy,
    SameAsInput,
    Unconfigured,
}

pub type DefaultOperatorName = SmallString<[u8; 16]>;

impl OperatorData {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        match self {
            OperatorData::Print(_) => "p".into(),
            OperatorData::Sequence(op) => op.default_op_name(),
            OperatorData::Fork(_) => "fork".into(),
            OperatorData::Foreach(_) => "foreach".into(),
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
            OperatorData::ToStr(op) => op.default_op_name(),
            OperatorData::Call(_) => "call".into(),
            OperatorData::CallConcurrent(_) => "callcc".into(),
            OperatorData::Nop(_) => "nop".into(),
            OperatorData::NopCopy(_) => "nop-c".into(),
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
            OperatorData::Print(_)
            | OperatorData::Sequence(_)
            | OperatorData::Regex(_)
            | OperatorData::FileReader(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::Literal(_)
            | OperatorData::Join(_)
            | OperatorData::Count(_)
            | OperatorData::ToStr(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Nop(_)
            | OperatorData::Aggregator(_)
            | OperatorData::NopCopy(_) => true,
            OperatorData::Fork(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Next(_)
            | OperatorData::End(_) => false,
            OperatorData::Custom(op) => op.can_be_appended(),
        }
    }
    pub fn output_field_kind(
        &self,
        op_base: &OperatorBase,
    ) -> OutputFieldKind {
        match self {
            OperatorData::Print(_)
            | OperatorData::Sequence(_)
            | OperatorData::Regex(_)
            | OperatorData::FileReader(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::Literal(_)
            | OperatorData::Join(_)
            | OperatorData::Count(_)
            | OperatorData::ToStr(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Aggregator(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Fork(_) => OutputFieldKind::Unique,
            OperatorData::Foreach(_) | OperatorData::Nop(_) => {
                OutputFieldKind::SameAsInput
            }
            OperatorData::ForkCat(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Next(_)
            | OperatorData::End(_) => OutputFieldKind::Unconfigured,
            OperatorData::Custom(op) => op.output_field_kind(op_base),
        }
    }
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> DefaultOperatorName;

    // mainly used for operators that start subchains
    // makes sure that e.g. `scr seqn=10 fork +int=11 p`
    // does not try to aggregate `fork` with `int`
    // `fork` cannot be appended directly (although `fork end +int=42` is
    // legal)
    fn can_be_appended(&self) -> bool {
        false
    }
    fn output_field_kind(&self, _op_base: &OperatorBase) -> OutputFieldKind {
        OutputFieldKind::Unique
    }
    fn output_count(&self, _op_base: &OperatorBase) -> usize;
    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool;
    fn on_op_added(&self, _sess: &mut SessionOptions) {}
    fn on_subchains_added(&mut self, _current_subchain_count: u32) {}
    fn register_output_var_names(
        &self,
        _ld: &mut LivenessData,
        _sess: &SessionData,
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
        _sess: &SessionData,
        _op_id: OperatorId,
        _ld: &LivenessData,
    ) {
    }
    // While lifetimes can be elided here, which is nice for simple TFs,
    // it's good to remember that `TransformData<'a>` comes from `&'a self`
    // and can take full advantage of that for sharing state between instances
    fn build_transform(
        &self,
        jd: &mut JobData,
        op_base: &OperatorBase,
        tf_state: &mut TransformState,
        prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData;
}

pub fn operator_build_transforms(
    job: &mut Job,
    mut tf_state: TransformState,
    op_id: OperatorId,
    prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
) -> OperatorInstantiation {
    let tfs = &mut tf_state;
    let jd = &mut job.job_data;
    let op_base = &jd.session_data.operator_bases[op_id as usize];
    let op_data = &jd.session_data.operator_data[op_id as usize];
    let tf_data = match op_data {
        OperatorData::Nop(op) => build_tf_nop(op, tfs),
        OperatorData::NopCopy(op) => build_tf_nop_copy(jd, op, tfs),
        OperatorData::ToStr(op) => build_tf_to_str(jd, op_base, op, tfs),
        OperatorData::Count(op) => build_tf_count(jd, op_base, op, tfs),
        OperatorData::Foreach(op) => {
            return insert_tf_foreach(
                job,
                op,
                tf_state,
                op_base.chain_id.unwrap(),
                op_id,
                prebound_outputs,
            );
        }
        OperatorData::Fork(op) => build_tf_fork(jd, op_base, op, tfs),
        OperatorData::ForkCat(op) => {
            return insert_tf_forkcat(job, op_base, op, tf_state);
        }
        OperatorData::Print(op) => build_tf_print(jd, op_base, op, tfs),
        OperatorData::Join(op) => build_tf_join(jd, op_base, op, tfs),
        OperatorData::Regex(op) => {
            build_tf_regex(jd, op_base, op, tfs, prebound_outputs)
        }
        OperatorData::Format(op) => build_tf_format(jd, op_base, op, tfs),
        OperatorData::StringSink(op) => {
            build_tf_string_sink(jd, op_base, op, tfs)
        }
        OperatorData::FieldValueSink(op) => {
            build_tf_field_value_sink(jd, op_base, op, tfs)
        }
        OperatorData::FileReader(op) => {
            build_tf_file_reader(jd, op_base, op, tfs)
        }
        OperatorData::Literal(op) => build_tf_literal(jd, op_base, op, tfs),
        OperatorData::Sequence(op) => build_tf_sequence(jd, op_base, op, tfs),
        OperatorData::Select(op) => build_tf_select(jd, op_base, op, tfs),
        OperatorData::Call(op) => build_tf_call(jd, op_base, op, tfs),
        OperatorData::CallConcurrent(op) => {
            build_tf_call_concurrent(jd, op_base, op, tfs)
        }
        OperatorData::Key(_)
        | OperatorData::Next(_)
        | OperatorData::End(_) => unreachable!(),
        OperatorData::Custom(op) => {
            op.build_transform(jd, op_base, tfs, prebound_outputs)
        }
        OperatorData::Aggregator(op) => {
            return insert_tf_aggregator(
                job,
                op,
                tf_state,
                op_id,
                prebound_outputs,
            );
        }
    };

    let next_input_field = if tf_state.is_transparent {
        tf_state.input_field
    } else {
        tf_state.output_field
    };
    let tf_id = add_transform_to_job(
        &mut job.job_data,
        &mut job.transform_data,
        tf_state,
        tf_data,
    );
    OperatorInstantiation {
        tfs_begin: tf_id,
        tfs_end: tf_id,
        next_input_field,
        continuation: TransformContinuationKind::Regular,
    }
}
