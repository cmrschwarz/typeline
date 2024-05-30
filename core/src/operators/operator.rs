use std::{collections::HashMap, fmt::Write};

use smallstr::SmallString;

use crate::{
    chain::ChainId,
    context::SessionData,
    job::{add_transform_to_job, Job},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect, VarId,
    },
    options::{
        argument::CliArgIdx, chain_options::ChainOptions,
        session_options::SessionOptions,
    },
    record_data::{field::FieldId, group_track::GroupTrackId},
    utils::{
        identity_hasher::BuildIdentityHasher, small_box::SmallBox,
        string_store::StringStoreEntry,
    },
};

use super::{
    aggregator::{
        insert_tf_aggregator, setup_op_aggregator, OpAggregator,
        AGGREGATOR_DEFAULT_NAME,
    },
    call::{build_tf_call, setup_op_call, OpCall},
    call_concurrent::{
        build_tf_call_concurrent, setup_op_call_concurrent,
        setup_op_call_concurrent_liveness_data, OpCallConcurrent,
    },
    count::{build_tf_count, OpCount},
    end::{setup_op_end, OpEnd},
    errors::OperatorSetupError,
    field_value_sink::{build_tf_field_value_sink, OpFieldValueSink},
    file_reader::{build_tf_file_reader, setup_op_file_reader, OpFileReader},
    foreach::{insert_tf_foreach, setup_op_foreach, OpForeach},
    fork::{
        build_tf_fork, setup_op_fork, setup_op_fork_liveness_data, OpFork,
    },
    forkcat::{
        insert_tf_forkcat, setup_op_forkcat, setup_op_forkcat_liveness_data,
        OpForkCat,
    },
    format::{
        build_tf_format, format_add_var_names, setup_op_format,
        update_op_format_variable_liveness, OpFormat,
    },
    join::{build_tf_join, OpJoin},
    key::{setup_op_key, OpKey},
    literal::{build_tf_literal, OpLiteral},
    multi_op::OpMultiOp,
    next::OpNext,
    nop::{build_tf_nop, setup_op_nop, OpNop},
    nop_copy::{
        build_tf_nop_copy, on_op_nop_copy_liveness_computed, OpNopCopy,
    },
    print::{build_tf_print, OpPrint},
    regex::{build_tf_regex, setup_op_regex, OpRegex},
    select::{
        build_tf_select, setup_op_select, setup_op_select_liveness_data,
        OpSelect,
    },
    sequence::{
        build_tf_sequence, setup_op_sequence_concurrent_liveness_data,
        update_op_sequence_variable_liveness, OpSequence,
    },
    string_sink::{build_tf_string_sink, OpStringSink},
    success_updater::{build_tf_success_updator, OpSuccessUpdator},
    to_str::{build_tf_to_str, OpToStr},
    transform::{TransformData, TransformId, TransformState},
};

pub type OperatorId = u32;
pub type OperatorOffsetInChain = u32;

pub type PreboundOutputsMap =
    HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>;

pub static DUMMY_OP_NOP: OperatorData = OperatorData::Nop(OpNop {});

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
    SuccessUpdator(OpSuccessUpdator),
    MultiOp(OpMultiOp),
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
    pub next_group_track: GroupTrackId,
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

impl Default for OperatorData {
    fn default() -> Self {
        OperatorData::Nop(OpNop {})
    }
}

impl OperatorData {
    pub fn setup(
        &mut self,
        sess: &mut SessionData,
        chain_id: ChainId,
        op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        let chain = &sess.chains[chain_id];
        let op_base = &mut sess.operator_bases[op_id];
        match self {
            OperatorData::Regex(op) => {
                setup_op_regex(op, sess.string_store.get_mut().unwrap())
            }
            OperatorData::Format(op) => {
                setup_op_format(op, sess.string_store.get_mut().unwrap())
            }
            OperatorData::Key(op) => {
                setup_op_key(op, sess.string_store.get_mut().unwrap())
            }
            OperatorData::Select(op) => {
                setup_op_select(op, sess.string_store.get_mut().unwrap())
            }
            OperatorData::FileReader(op) => setup_op_file_reader(chain, op),
            OperatorData::Fork(op) => setup_op_fork(chain, op_base, op, op_id),
            OperatorData::Foreach(op) => setup_op_foreach(op, chain_id, op_id),
            OperatorData::Nop(op) => setup_op_nop(chain, op_base, op, op_id),
            OperatorData::ForkCat(op) => {
                setup_op_forkcat(chain, op_base, op, op_id)
            }

            OperatorData::Next(_) => unreachable!(),
            OperatorData::End(op) => setup_op_end(op, op_id),
            OperatorData::Call(op) => setup_op_call(
                &sess.chain_labels,
                sess.string_store.get_mut().unwrap(),
                op,
                op_id,
            ),
            OperatorData::CallConcurrent(op) => setup_op_call_concurrent(
                &sess.settings,
                &sess.chain_labels,
                sess.string_store.get_mut().unwrap(),
                op,
                op_id,
            ),
            OperatorData::Custom(op) => {
                Operator::setup(&mut **op, sess, chain_id, op_id)
            }
            OperatorData::MultiOp(op) => {
                Operator::setup(op, sess, chain_id, op_id)
            }
            OperatorData::Aggregator(op) => {
                setup_op_aggregator(op, sess, chain_id)
            }
            OperatorData::ToStr(_)
            | OperatorData::Count(_)
            | OperatorData::Sequence(_)
            | OperatorData::Literal(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::NopCopy(_)
            | OperatorData::StringSink(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::FieldValueSink(_) => Ok(()),
        }
    }
    pub fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> bool {
        match self {
            OperatorData::Nop(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::ToStr(_)
            | OperatorData::Count(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Next(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::End(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Sequence(_)
            | OperatorData::Aggregator(_)
            | OperatorData::Foreach(_) => false,
            OperatorData::MultiOp(op) => {
                Operator::has_dynamic_outputs(op, sess, op_id)
            }
            OperatorData::Custom(op) => {
                Operator::has_dynamic_outputs(&**op, sess, op_id)
            }
        }
    }
    pub fn output_count(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> usize {
        #[allow(clippy::match_same_arms)]
        match &self {
            OperatorData::Call(_) => 1,
            OperatorData::CallConcurrent(_) => 1,
            OperatorData::ToStr(_) => 1,
            OperatorData::Count(_) => 1,
            OperatorData::Print(_) => 1,
            OperatorData::Join(_) => 1,
            OperatorData::Fork(_) => 0,
            OperatorData::Nop(_) => 0,
            OperatorData::SuccessUpdator(_) => 0,
            OperatorData::NopCopy(_) => 1,
            // technically this has output, but it always introduces a
            // separate BB so we don't want to allocate slots for that
            OperatorData::ForkCat(_) => 0,
            OperatorData::Next(_) => 0,
            OperatorData::End(_) => 0,
            OperatorData::Key(_) => 1,
            OperatorData::Select(_) => 0,
            OperatorData::Regex(re) => {
                re.capture_group_names
                    .iter()
                    .map(|n| n.map(|_| 1).unwrap_or(0))
                    .sum::<usize>()
                    + 1
            }
            OperatorData::Format(_) => 1,
            OperatorData::StringSink(_) => 1,
            OperatorData::FieldValueSink(_) => 1,
            OperatorData::FileReader(_) => 1,
            OperatorData::Literal(_) => 1,
            OperatorData::Sequence(_) => 1,
            OperatorData::Foreach(_) => 0, // last sc output is output
            OperatorData::Aggregator(agg) => {
                let mut op_count = 1;
                // TODO: do this properly, merging field names etc.
                for &sub_op in &agg.sub_ops {
                    op_count += sess.operator_data[sub_op]
                        .output_count(sess, sub_op)
                        .saturating_sub(1);
                }
                op_count
            }
            OperatorData::Custom(op) => {
                Operator::output_count(&**op, sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                Operator::output_count(op, sess, op_id)
            }
        }
    }

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
            OperatorData::SuccessUpdator(_) => "success_updator".into(),
            OperatorData::NopCopy(_) => "nop-c".into(),
            OperatorData::Custom(op) => op.default_name(),
            OperatorData::Aggregator(_) => AGGREGATOR_DEFAULT_NAME.into(),
            OperatorData::MultiOp(op) => op.default_name(),
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
            | OperatorData::SuccessUpdator(_)
            | OperatorData::Aggregator(_)
            | OperatorData::NopCopy(_) => true,
            OperatorData::Fork(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Next(_)
            | OperatorData::End(_) => false,
            OperatorData::Custom(op) => Operator::can_be_appended(&**op),
            OperatorData::MultiOp(op) => Operator::can_be_appended(op),
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
            OperatorData::Foreach(_)
            | OperatorData::Nop(_)
            | OperatorData::SuccessUpdator(_) => OutputFieldKind::SameAsInput,
            OperatorData::ForkCat(_)
            | OperatorData::Key(_)
            | OperatorData::Select(_)
            | OperatorData::Next(_)
            | OperatorData::End(_) => OutputFieldKind::Unconfigured,
            OperatorData::Custom(op) => {
                Operator::output_field_kind(&**op, op_base)
            }
            OperatorData::MultiOp(op) => {
                Operator::output_field_kind(op, op_base)
            }
        }
    }
    pub fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        op_id: OperatorId,
    ) {
        match &self {
            OperatorData::Regex(re) => {
                for n in &re.capture_group_names {
                    ld.add_var_name_opt(*n);
                }
            }
            OperatorData::Key(k) => {
                ld.add_var_name(k.key_interned.unwrap());
            }
            OperatorData::Select(s) => {
                ld.add_var_name(s.key_interned.unwrap());
            }
            OperatorData::Format(fmt) => format_add_var_names(fmt, ld),
            OperatorData::Aggregator(agg) => {
                for &sub_op in &agg.sub_ops {
                    ld.setup_op_vars(sess, sub_op);
                }
            }
            OperatorData::Custom(op) => {
                op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::ToStr(_)
            | OperatorData::Count(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Next(_)
            | OperatorData::End(_)
            | OperatorData::Nop(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::Foreach(_)
            | OperatorData::NopCopy(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Sequence(_) => (),
        }
    }
    pub fn update_liveness_for_op(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        flags: &mut AccessFlags,
        op_offset_after_last_write: OperatorOffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
        outputs_offset: OpOutputIdx,
    ) -> (OpOutputIdx, OperatorCallEffect) {
        let output_field = sess.operator_bases[op_id].outputs_start
            + outputs_offset as OpOutputIdx;
        match &self {
            OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Foreach(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_) => {
                return (output_field, OperatorCallEffect::Diverge);
            }
            OperatorData::Key(key) => {
                let var_id = ld.var_names[&key.key_interned.unwrap()];
                ld.vars_to_op_outputs_map[var_id] = output_field;
                ld.op_outputs[output_field]
                    .field_references
                    .push(input_field);
                if let Some(prev_tgt) =
                    ld.key_aliases_map.insert(var_id, input_field)
                {
                    ld.apply_var_remapping(var_id, prev_tgt);
                }
                return (input_field, OperatorCallEffect::NoCall);
            }
            OperatorData::Select(select) => {
                let mut var = ld.var_names[&select.key_interned.unwrap()];
                // resolve rebinds
                loop {
                    let field = ld.vars_to_op_outputs_map[var];
                    if field >= ld.vars.len() as OpOutputIdx {
                        break;
                    }
                    // var points to itself
                    if field == var {
                        break;
                    }
                    // OpOutput indices below vars.len() are the vars
                    var = field as VarId;
                }
                return (var, OperatorCallEffect::NoCall);
            }
            OperatorData::Regex(re) => {
                flags.may_dup_or_drop =
                    !re.opts.non_mandatory || re.opts.multimatch;
                flags.non_stringified_input_access = false;
                for i in 0..re.capture_group_names.len() {
                    ld.op_outputs[output_field + i as OpOutputIdx]
                        .field_references
                        .push(input_field);
                }

                for (cgi, cgn) in re.capture_group_names.iter().enumerate() {
                    if let Some(name) = cgn {
                        let tgt_var_name = ld.var_names[name];
                        ld.vars_to_op_outputs_map[tgt_var_name] =
                            sess.operator_bases[op_id].outputs_start
                                + cgi as OpOutputIdx;
                    }
                }
            }
            OperatorData::NopCopy(_) => {
                flags.may_dup_or_drop = false;
                flags.non_stringified_input_access = false;
                ld.op_outputs[output_field]
                    .field_references
                    .push(input_field);
            }
            OperatorData::Format(fmt) => {
                update_op_format_variable_liveness(
                    sess,
                    fmt,
                    ld,
                    op_id,
                    flags,
                    op_offset_after_last_write,
                );
            }
            OperatorData::FileReader(_) | OperatorData::Count(_) => {
                // this only inserts if input is done, so no write flag
                // neccessary
                flags.input_accessed = false;
                flags.non_stringified_input_access = false;
            }
            OperatorData::Literal(di) => {
                flags.may_dup_or_drop = di.insert_count.is_some();
                flags.input_accessed = false;
                flags.non_stringified_input_access = false;
            }
            OperatorData::Join(_) => {}
            OperatorData::Sequence(seq) => {
                update_op_sequence_variable_liveness(flags, seq);
            }
            OperatorData::Nop(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::StringSink(_)
            | OperatorData::Print(_) => {
                flags.may_dup_or_drop = false;
                flags.non_stringified_input_access = false;
            }
            OperatorData::FieldValueSink(_) | OperatorData::ToStr(_) => {
                flags.may_dup_or_drop = false;
            }
            OperatorData::Next(_) | OperatorData::End(_) => unreachable!(),
            OperatorData::Custom(op) => {
                if let Some(res) = Operator::update_variable_liveness(
                    &**op,
                    sess,
                    ld,
                    flags,
                    op_offset_after_last_write,
                    op_id,
                    bb_id,
                    input_field,
                ) {
                    return res;
                }
            }
            OperatorData::MultiOp(op) => {
                if let Some(res) = Operator::update_variable_liveness(
                    op,
                    sess,
                    ld,
                    flags,
                    op_offset_after_last_write,
                    op_id,
                    bb_id,
                    input_field,
                ) {
                    return res;
                }
            }
            OperatorData::Aggregator(op) => {
                flags.may_dup_or_drop = false;
                flags.non_stringified_input_access = false;
                flags.input_accessed = false;
                for &sub_op in &op.sub_ops {
                    let mut sub_op_flags = AccessFlags {
                        input_accessed: true,
                        non_stringified_input_access: true,
                        may_dup_or_drop: true,
                    };
                    sess.operator_data[sub_op].update_liveness_for_op(
                        sess,
                        ld,
                        &mut sub_op_flags,
                        op_offset_after_last_write,
                        sub_op,
                        bb_id,
                        input_field,
                        0,
                    );
                    *flags = flags.or(&sub_op_flags);
                    let sub_op = &sess.operator_bases[sub_op];
                    if sub_op.outputs_start != sub_op.outputs_end {
                        ld.op_outputs[output_field]
                            .field_references
                            .push(sub_op.outputs_start);
                    }
                }
            }
        }
        (output_field, OperatorCallEffect::Basic)
    }
    pub fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        match self {
            OperatorData::CallConcurrent(op) => {
                setup_op_call_concurrent_liveness_data(op, op_id, ld)
            }
            OperatorData::NopCopy(op) => {
                on_op_nop_copy_liveness_computed(op, op_id, ld)
            }
            OperatorData::Fork(op) => {
                setup_op_fork_liveness_data(op, op_id, ld)
            }
            OperatorData::ForkCat(op) => {
                setup_op_forkcat_liveness_data(sess, op, op_id, ld)
            }
            OperatorData::Select(op) => {
                setup_op_select_liveness_data(op, op_id, ld)
            }
            OperatorData::Sequence(op) => {
                setup_op_sequence_concurrent_liveness_data(sess, op, op_id, ld)
            }
            OperatorData::MultiOp(op) => {
                op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::Custom(op) => {
                op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::Aggregator(agg) => {
                for &sc_id in &agg.sub_ops {
                    SessionOptions::setup_op_liveness(sess, ld, sc_id);
                }
            }
            OperatorData::ToStr(_)
            | OperatorData::Call(_)
            | OperatorData::Nop(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::Count(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Foreach(_)
            | OperatorData::Next(_)
            | OperatorData::End(_)
            | OperatorData::Key(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_) => (),
        }
    }
    pub fn on_op_added(
        &mut self,
        so: &mut SessionOptions,
        op_id: OperatorId,
        add_to_chain: bool,
    ) {
        let chain_opts = &mut so.chains[so.curr_chain];
        let op_base_opts = &mut so.operator_base_options[op_id];
        match self {
            OperatorData::CallConcurrent(_) => {
                so.any_threaded_operations = true;
            }
            OperatorData::Foreach(OpForeach {
                subchains_start, ..
            })
            | OperatorData::Fork(OpFork {
                subchains_start, ..
            })
            | OperatorData::ForkCat(OpForkCat {
                subchains_start, ..
            }) => {
                *subchains_start = so.chains[so.curr_chain].subchain_count;
                so.chains[so.curr_chain].subchain_count += 1;
                let new_chain = ChainOptions {
                    parent: so.curr_chain,
                    ..Default::default()
                };
                so.curr_chain = so.chains.len() as ChainId;
                so.chains.push(new_chain);
            }
            OperatorData::Next(_) => {
                if add_to_chain {
                    chain_opts.operators.pop();
                }
                let parent = so.chains[so.curr_chain].parent;
                so.chains[parent].subchain_count += 1;
                let new_chain = ChainOptions {
                    parent,
                    ..Default::default()
                };
                so.curr_chain = so.chains.len() as ChainId;
                so.chains.push(new_chain);
                op_base_opts.chain_id = None;
            }
            OperatorData::End(end) => {
                if add_to_chain {
                    chain_opts.operators.pop();
                }
                end.chain_id_before = so.curr_chain;
                // the parent of the root chain is itsess
                so.curr_chain = so.chains[so.curr_chain].parent;

                op_base_opts.chain_id = Some(so.curr_chain);
                end.subchain_count_after =
                    so.chains[so.curr_chain].subchain_count;
                let subchain_count_after = end.subchain_count_after;
                if let Some(&op_id) = so.chains[so.curr_chain].operators.last()
                {
                    SessionOptions::on_operator_subchains_ended(
                        &mut so.operator_data[op_id],
                        subchain_count_after,
                    );
                }
            }
            OperatorData::Aggregator(agg) => {
                for i in 0..agg.sub_ops.len() {
                    let sub_op_id = agg.sub_ops[i];
                    so.init_op(sub_op_id, false);
                }
            }
            OperatorData::Custom(op) => {
                Operator::on_op_added(&mut **op, so, op_id, add_to_chain);
            }
            OperatorData::MultiOp(op) => {
                Operator::on_op_added(op, so, op_id, add_to_chain)
            }
            OperatorData::Print(_)
            | OperatorData::Call(_)
            | OperatorData::Count(_)
            | OperatorData::ToStr(_)
            | OperatorData::Key(_)
            | OperatorData::Nop(_)
            | OperatorData::SuccessUpdator(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Sequence(_)
            | OperatorData::Join(_) => (),
        }
    }
    pub fn operator_build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        mut tf_state: TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> OperatorInstantiation {
        let tfs = &mut tf_state;
        let jd = &mut job.job_data;
        let op_base = &jd.session_data.operator_bases[op_id];
        let data: TransformData<'a> = match self {
            OperatorData::Nop(op) => build_tf_nop(op, tfs),
            OperatorData::SuccessUpdator(op) => {
                build_tf_success_updator(jd, op, tfs)
            }
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
            OperatorData::Literal(op) => {
                build_tf_literal(jd, op_base, op, tfs)
            }
            OperatorData::Sequence(op) => {
                build_tf_sequence(jd, op_base, op, tfs)
            }
            OperatorData::Select(op) => build_tf_select(jd, op_base, op, tfs),
            OperatorData::Call(op) => build_tf_call(jd, op_base, op, tfs),
            OperatorData::CallConcurrent(op) => {
                build_tf_call_concurrent(jd, op_base, op, tfs)
            }
            OperatorData::Key(_)
            | OperatorData::Next(_)
            | OperatorData::End(_) => unreachable!(),
            OperatorData::Custom(op) => {
                match Operator::build_transforms(
                    &**op,
                    job,
                    tfs,
                    op_id,
                    prebound_outputs,
                ) {
                    TransformInstatiation::Simple(tf) => tf,
                    TransformInstatiation::Multi(instantiation) => {
                        return instantiation
                    }
                }
            }
            OperatorData::MultiOp(op) => {
                match Operator::build_transforms(
                    op,
                    job,
                    tfs,
                    op_id,
                    prebound_outputs,
                ) {
                    TransformInstatiation::Simple(tf) => tf,
                    TransformInstatiation::Multi(instantiation) => {
                        return instantiation
                    }
                }
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
        let next_group_track = tf_state.output_group_track_id;
        let tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state,
            data,
        );
        OperatorInstantiation {
            tfs_begin: tf_id,
            tfs_end: tf_id,
            next_input_field,
            next_group_track,
            continuation: TransformContinuationKind::Regular,
        }
    }
}

pub enum TransformInstatiation<'a> {
    Simple(TransformData<'a>),
    // TODO: rename this
    Multi(OperatorInstantiation),
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> DefaultOperatorName;

    // mainly used for operators that start subchains
    // makes sure that e.g. `scr seqn=10 fork +int=11 p`
    // does not try to aggregate `fork` with `int`
    // `fork` cannot be appended directly (although `fork end +int=42` is
    // legal)
    fn can_be_appended(&self) -> bool {
        true
    }
    fn output_field_kind(&self, _op_base: &OperatorBase) -> OutputFieldKind {
        OutputFieldKind::Unique
    }
    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize;
    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool;
    fn on_op_added(
        &mut self,
        _so: &mut SessionOptions,
        _op_id: OperatorId,
        _add_to_chain: bool,
    ) {
    }
    fn on_subchains_added(&mut self, _current_subchain_count: u32) {}
    fn register_output_var_names(
        &self,
        _ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
    }

    // all of the &mut bool flags default to true
    // turning them to false allows for some pipeline optimizations
    // but may cause incorrect behavior if the promises made are broken later
    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OperatorOffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)>;
    fn setup(
        &mut self,
        _sess: &mut SessionData,
        _chain_id: OperatorId,
        _op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        Ok(())
    }
    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        _ld: &LivenessData,
        _op_id: OperatorId,
    ) {
    }
    // While lifetimes can be elided here, which is nice for simple TFs,
    // it's good to remember that `TransformData<'a>` comes from `&'a self`
    // and can take full advantage of that for sharing state between instances
    fn build_transforms<'a>(
        &'a self,
        job: &mut Job<'a>,
        tf_state: &mut TransformState,
        op_id: OperatorId,
        prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation;
}
