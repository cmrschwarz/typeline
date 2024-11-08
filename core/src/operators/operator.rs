use std::collections::HashMap;

use smallstr::SmallString;

use crate::{
    chain::{Chain, ChainId, SubchainIndex},
    cli::call_expr::Span,
    context::SessionData,
    index_newtype,
    job::{add_transform_to_job, Job},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorCallEffect,
        OperatorLivenessOutput, VarId,
    },
    options::session_setup::SessionSetupData,
    record_data::{
        field::FieldId, group_track::GroupTrackId, match_set::MatchSetId,
    },
    scr_error::ScrError,
    smallbox,
    utils::{
        identity_hasher::BuildIdentityHasher, indexing_type::IndexingType,
        small_box::SmallBox,
    },
};

use super::{
    atom::{setup_op_atom, OpAtom},
    call::{build_tf_call, setup_op_call, OpCall},
    call_concurrent::{
        build_tf_call_concurrent, setup_op_call_concurrent,
        setup_op_call_concurrent_liveness_data, OpCallConcurrent,
    },
    chunks::{insert_tf_chunks, setup_op_chunks, OpChunks},
    compute::{
        build_tf_compute, compute_add_var_names, setup_op_compute,
        update_op_compute_variable_liveness, OpCompute,
    },
    field_value_sink::{build_tf_field_value_sink, OpFieldValueSink},
    file_reader::{build_tf_file_reader, setup_op_file_reader, OpFileReader},
    foreach::{insert_tf_foreach, setup_op_foreach, OpForeach},
    foreach_unique::{
        insert_tf_foreach_unique, setup_op_foreach_unique, OpForeachUnique,
    },
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
    macro_call::{
        macro_call_has_dynamic_outputs, setup_op_macro_call, OpMacroCall,
    },
    macro_def::{setup_op_macro_def, OpMacroDef},
    multi_op::OpMultiOp,
    nop::{build_tf_nop, setup_op_nop, OpNop},
    nop_copy::{
        build_tf_nop_copy, on_op_nop_copy_liveness_computed, OpNopCopy,
    },
    print::{build_tf_print, OpPrint},
    regex::{build_tf_regex, regex_output_counts, setup_op_regex, OpRegex},
    select::{setup_op_select, OpSelect},
    string_sink::{build_tf_string_sink, OpStringSink},
    transform::{TransformData, TransformId, TransformState},
    transparent::{build_tf_transparent, setup_op_transparent, OpTransparent},
    utils::nested_op::{setup_op_outputs_for_nested_op, NestedOp},
};

index_newtype! {
    pub struct OperatorId(u32);
    pub struct OperatorDataId(u32);
    pub struct OffsetInChain(u32);
    pub struct OffsetInAggregation(u32);
    pub struct OffsetInChainOptions(u32);
}

pub type PreboundOutputsMap =
    HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>;

pub static DUMMY_OP_NOP: OperatorData = OperatorData::Nop(OpNop {});

pub enum OperatorData {
    Nop(OpNop),
    NopCopy(OpNopCopy),
    Call(OpCall),
    CallConcurrent(OpCallConcurrent),
    Print(OpPrint),
    Join(OpJoin),
    Fork(OpFork),
    ForkCat(OpForkCat),
    Key(OpKey),
    Atom(OpAtom),
    Transparent(OpTransparent),
    Select(OpSelect),
    Regex(OpRegex),
    Format(OpFormat),
    Compute(OpCompute),
    StringSink(OpStringSink),
    FieldValueSink(OpFieldValueSink),
    FileReader(OpFileReader),
    Literal(OpLiteral),
    Foreach(OpForeach),
    ForeachUnique(OpForeachUnique),
    Chunks(OpChunks),
    MacroDef(OpMacroDef),
    MacroCall(OpMacroCall),
    MultiOp(OpMultiOp),
    Custom(SmallBox<dyn Operator, 96>),
}

#[derive(Clone, Copy)]
pub enum OperatorOffsetInChain {
    Direct(OffsetInChain),
    AggregationMember(OperatorId, OffsetInAggregation),
}

pub struct OperatorBase {
    pub op_data_id: OperatorDataId,

    pub chain_id: ChainId,
    pub offset_in_chain: OperatorOffsetInChain,
    pub desired_batch_size: usize,

    pub span: Span,

    // these two are not part of the OperatorLivenessData struct because it is
    // used in the `prebound_outputs` mechanism that is used during
    // operators -> transforms expansion long after liveness analysis
    // has concluded
    pub outputs_start: OpOutputIdx,
    pub outputs_end: OpOutputIdx,
}

pub struct OperatorInstantiation {
    pub tfs_begin: TransformId,
    pub tfs_end: TransformId,
    pub next_match_set: MatchSetId,
    pub next_input_field: FieldId,
    pub next_group_track: GroupTrackId,
}

#[derive(Default, PartialEq, Eq, Clone, Copy)]
pub enum OutputFieldKind {
    #[default]
    Unique,
    Dummy,
    SameAsInput,
    Unconfigured,
}

pub type OperatorName = SmallString<[u8; 32]>;

impl Default for OperatorData {
    fn default() -> Self {
        OperatorData::Nop(OpNop {})
    }
}

impl OperatorBase {
    pub fn new(
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        desired_batch_size: usize,
        span: Span,
    ) -> Self {
        Self {
            chain_id,
            offset_in_chain,
            desired_batch_size,
            span,
            op_data_id: OperatorDataId::MAX_VALUE,
            outputs_start: OpOutputIdx::MAX_VALUE,
            outputs_end: OpOutputIdx::MAX_VALUE,
        }
    }
}

impl OperatorOffsetInChain {
    pub fn base_chain_offset(&self, sess: &SessionData) -> OffsetInChain {
        match self {
            OperatorOffsetInChain::Direct(chain_offset) => *chain_offset,
            OperatorOffsetInChain::AggregationMember(op_id, _agg_offset) => {
                sess.operator_bases[*op_id]
                    .offset_in_chain
                    .base_chain_offset(sess)
            }
        }
    }
}

impl OperatorData {
    pub fn from_custom(op: impl Operator + 'static) -> Self {
        Self::Custom(smallbox!(op))
    }
    pub fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        match self {
            OperatorData::Regex(op) => setup_op_regex(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Format(op) => setup_op_format(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Compute(op) => setup_op_compute(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Key(op) => setup_op_key(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Transparent(op) => setup_op_transparent(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Select(op) => setup_op_select(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::FileReader(op) => setup_op_file_reader(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Fork(op) => setup_op_fork(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Foreach(op) => setup_op_foreach(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::ForeachUnique(op) => setup_op_foreach_unique(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Chunks(op) => setup_op_chunks(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Nop(op) => setup_op_nop(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::ForkCat(op) => setup_op_forkcat(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Call(op) => setup_op_call(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::CallConcurrent(op) => setup_op_call_concurrent(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Custom(op) => Operator::setup(
                &mut **op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::MultiOp(op) => Operator::setup(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::MacroDef(op) => setup_op_macro_def(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::MacroCall(op) => setup_op_macro_call(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Atom(op) => setup_op_atom(
                op,
                sess,
                op_data_id,
                chain_id,
                offset_in_chain,
                span,
            ),
            OperatorData::Literal(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::NopCopy(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_) => {
                Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
            }
        }
    }
    pub fn has_dynamic_outputs(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> bool {
        match self {
            OperatorData::Nop(_)
            | OperatorData::Atom(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::Compute(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::MacroDef(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_) => false,
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return false;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                self.has_dynamic_outputs(sess, op_id)
            }
            OperatorData::Transparent(op) => {
                let NestedOp::SetUp(op_id) = op.nested_op else {
                    unreachable!()
                };
                self.has_dynamic_outputs(sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                Operator::has_dynamic_outputs(op, sess, op_id)
            }
            OperatorData::MacroCall(op) => {
                macro_call_has_dynamic_outputs(op, sess, op_id)
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
            OperatorData::Print(_) => 1,
            OperatorData::Join(_) => 1,
            OperatorData::Fork(_) => 0,
            OperatorData::Nop(_) => 0,
            OperatorData::NopCopy(_) => 1,
            // technically this has output, but it always introduces a
            // separate BB so we don't want to allocate slots for that
            OperatorData::ForkCat(_) => 0,
            OperatorData::Atom(_) => 0,
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return 0;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_count(sess, op_id)
            }
            OperatorData::Transparent(op) => {
                let NestedOp::SetUp(op_id) = op.nested_op else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_count(sess, op_id)
            }
            OperatorData::Select(_) => 0,
            OperatorData::Regex(re) => regex_output_counts(re),
            OperatorData::Format(_) => 1,
            OperatorData::Compute(_) => 1,
            OperatorData::StringSink(_) => 1,
            OperatorData::FieldValueSink(_) => 1,
            OperatorData::FileReader(_) => 1,
            OperatorData::Literal(_) => 1,
            OperatorData::Foreach(_) => 0,
            OperatorData::ForeachUnique(_) => 0,
            OperatorData::Chunks(_) => 0, // last sc output is output
            OperatorData::Custom(op) => {
                Operator::output_count(&**op, sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                Operator::output_count(op, sess, op_id)
            }
            OperatorData::MacroCall(op) => {
                op.op_multi_op.output_count(sess, op_id)
            }
            OperatorData::MacroDef(_) => 0,
        }
    }

    pub fn assign_op_outputs(
        &mut self,
        sess: &mut SessionData,
        ld: &mut LivenessData,
        op_id: OperatorId,
        output_count: &mut OpOutputIdx,
    ) {
        match self {
            OperatorData::Key(op) => {
                if let Some(nested_op) = &op.nested_op {
                    setup_op_outputs_for_nested_op(
                        nested_op,
                        sess,
                        ld,
                        op_id,
                        output_count,
                    );
                    return;
                }
            }
            OperatorData::Transparent(op) => {
                setup_op_outputs_for_nested_op(
                    &op.nested_op,
                    sess,
                    ld,
                    op_id,
                    output_count,
                );
                return;
            }
            OperatorData::Nop(_)
            | OperatorData::Atom(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::Compute(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::MacroDef(_)
            | OperatorData::MacroCall(_)
            | OperatorData::MultiOp(_) => (),
            OperatorData::Custom(op) => {
                op.assign_op_outputs(sess, ld, op_id, output_count)
            }
        }

        let op_output_count = self.output_count(sess, op_id);
        let op_base = &mut sess.operator_bases[op_id];
        op_base.outputs_start = *output_count;
        *output_count += OpOutputIdx::from_usize(op_output_count);
        op_base.outputs_end = *output_count;
        ld.append_op_outputs(op_output_count, op_id);
    }

    pub fn default_op_name(&self) -> OperatorName {
        match self {
            OperatorData::Atom(_) => "atom".into(),
            OperatorData::Print(_) => "p".into(),
            OperatorData::Fork(_) => "fork".into(),
            OperatorData::Foreach(_) => "foreach".into(),
            OperatorData::ForeachUnique(_) => "foreach-u".into(),
            OperatorData::Chunks(_) => "chunks".into(),
            OperatorData::ForkCat(_) => "forkcat".into(),
            OperatorData::Key(_) => "key".into(),
            OperatorData::Transparent(_) => "transparent".into(),
            OperatorData::Regex(_) => "regex".into(),
            OperatorData::FileReader(op) => op.default_op_name(),
            OperatorData::Format(_) => "f".into(),
            OperatorData::Compute(_) => "c".into(),
            OperatorData::Select(_) => "select".into(),
            OperatorData::Literal(op) => op.default_op_name(),
            OperatorData::Call(_) => "call".into(),
            OperatorData::CallConcurrent(_) => "callcc".into(),
            OperatorData::Nop(_) => "nop".into(),
            OperatorData::NopCopy(_) => "nop_copy".into(),
            OperatorData::Join(_) => "join".into(),
            OperatorData::StringSink(_) => "<string_sink>".into(),
            OperatorData::FieldValueSink(_) => "<field_value_sink>".into(),
            OperatorData::MultiOp(_) => "<multi_op>".into(),
            OperatorData::Custom(op) => op.default_name(),
            OperatorData::MacroDef(_) => "macro".into(),
            OperatorData::MacroCall(op) => op.name.clone().into(),
        }
    }
    pub fn debug_op_name(&self) -> OperatorName {
        match self {
            OperatorData::MultiOp(op) => op.debug_op_name(),
            OperatorData::Regex(op) => op.debug_op_name(),
            OperatorData::Join(op) => op.debug_op_name(),
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return self.default_op_name();
                };
                match nested {
                    NestedOp::Operator(nested_op) => {
                        format!(
                            "[ key '{}' {} ]",
                            op.key,
                            nested_op.0.debug_op_name()
                        )
                    }
                    NestedOp::SetUp(op_id) => {
                        format!("[ key '{}' <op {op_id:02}> ]", op.key)
                    }
                }
                .into()
            }
            _ => self.default_op_name(),
        }
    }
    pub fn output_field_kind(
        &self,
        sess: &SessionData,
        op_id: OperatorId,
    ) -> OutputFieldKind {
        match self {
            OperatorData::Print(_)
            | OperatorData::Regex(_)
            | OperatorData::FileReader(_)
            | OperatorData::Format(_)
            | OperatorData::Compute(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::Literal(_)
            | OperatorData::Join(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::NopCopy(_) => OutputFieldKind::Unique,
            OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::Nop(_)
            | OperatorData::Fork(_)
            | OperatorData::MacroDef(_) => OutputFieldKind::SameAsInput,
            OperatorData::ForkCat(_)
            | OperatorData::Select(_)
            | OperatorData::Atom(_) => OutputFieldKind::Unconfigured,
            OperatorData::Key(op) => {
                let Some(nested) = &op.nested_op else {
                    return OutputFieldKind::SameAsInput;
                };
                let &NestedOp::SetUp(op_id) = nested else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_field_kind(sess, op_id)
            }
            OperatorData::Transparent(op) => {
                let NestedOp::SetUp(op_id) = op.nested_op else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .output_field_kind(sess, op_id)
            }
            OperatorData::Custom(op) => {
                Operator::output_field_kind(&**op, sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                Operator::output_field_kind(op, sess, op_id)
            }
            OperatorData::MacroCall(op) => {
                op.op_multi_op.output_field_kind(sess, op_id)
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
                if let Some(NestedOp::SetUp(op_id)) = k.nested_op {
                    sess.operator_data[sess.op_data_id(op_id)]
                        .register_output_var_names(ld, sess, op_id);
                }
            }
            OperatorData::Transparent(k) => {
                let NestedOp::SetUp(op_id) = k.nested_op else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .register_output_var_names(ld, sess, op_id);
            }
            OperatorData::Select(s) => {
                ld.add_var_name(s.key_interned.unwrap());
            }
            OperatorData::Format(fmt) => format_add_var_names(fmt, ld),
            OperatorData::Compute(c) => compute_add_var_names(c, ld),
            OperatorData::Custom(op) => {
                op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::MultiOp(op) => {
                op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::MacroCall(op) => {
                op.op_multi_op.register_output_var_names(ld, sess, op_id)
            }
            OperatorData::Call(_)
            | OperatorData::Atom(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Nop(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::NopCopy(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::MacroDef(_) => (),
        }
    }
    pub fn update_liveness_for_op(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        bb_id: BasicBlockId,
        input_field: OpOutputIdx,
        outputs_offset: usize,
        output: &mut OperatorLivenessOutput,
    ) {
        let primary_output_idx = OpOutputIdx::from_usize(
            sess.operator_bases[op_id].outputs_start.into_usize()
                + outputs_offset,
        );
        match &self {
            OperatorData::Atom(_) => {
                output.flags.may_dup_or_drop = false;
                output.flags.non_stringified_input_access = false;
                output.flags.input_accessed = false;
            }
            // TODO: this shouldn't access inputs. fix testcases
            OperatorData::Nop(_)
            | OperatorData::StringSink(_)
            | OperatorData::Print(_) => {
                output.flags.may_dup_or_drop = false;
                output.flags.non_stringified_input_access = false;
            }
            OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::Call(_)
            | OperatorData::MacroDef(_)
            | OperatorData::CallConcurrent(_) => {
                output.call_effect = OperatorCallEffect::Diverge;
            }
            OperatorData::Key(key) => {
                if let Some(NestedOp::SetUp(nested_op_id)) = key.nested_op {
                    sess.operator_data[sess.op_data_id(nested_op_id)]
                        .update_liveness_for_op(
                            sess,
                            ld,
                            op_offset_after_last_write,
                            nested_op_id,
                            bb_id,
                            input_field,
                            outputs_offset,
                            output,
                        );
                }

                let var_id = ld.var_names[&key.key_interned.unwrap()];
                ld.vars_to_op_outputs_map[var_id] = primary_output_idx;
                ld.op_outputs[primary_output_idx]
                    .field_references
                    .push(input_field);
                if let Some(prev_tgt) =
                    ld.key_aliases_map.insert(var_id, input_field)
                {
                    ld.apply_var_remapping(var_id, prev_tgt);
                }
                output.primary_output = input_field;
                output.call_effect = OperatorCallEffect::NoCall;
            }
            OperatorData::Transparent(op) => {
                let NestedOp::SetUp(nested_op_id) = op.nested_op else {
                    unreachable!()
                };
                sess.operator_data[sess.op_data_id(op_id)]
                    .update_liveness_for_op(
                        sess,
                        ld,
                        op_offset_after_last_write,
                        nested_op_id,
                        bb_id,
                        input_field,
                        outputs_offset,
                        output,
                    );
                output.primary_output = input_field;
            }
            OperatorData::Select(select) => {
                let mut var = ld.var_names[&select.key_interned.unwrap()];
                // resolve rebinds
                loop {
                    let field = ld.vars_to_op_outputs_map[var];
                    if field.into_usize() >= ld.vars.len() {
                        break;
                    }
                    // var points to itself
                    if field.into_usize() == var.into_usize() {
                        break;
                    }
                    // OpOutput indices below vars.len() are the vars
                    var = VarId::from_usize(field.into_usize());
                }
                output.primary_output = var.natural_output_idx();
                output.call_effect = OperatorCallEffect::NoCall;
            }
            OperatorData::Regex(re) => {
                output.flags.may_dup_or_drop =
                    !re.opts.non_mandatory || re.opts.multimatch;
                output.flags.non_stringified_input_access = false;
                for i in 0..re.capture_group_names.len() {
                    ld.op_outputs[OpOutputIdx::from_usize(
                        primary_output_idx.into_usize() + i,
                    )]
                    .field_references
                    .push(input_field);
                }

                for (cg_idx, cg_name) in
                    re.capture_group_names.iter().enumerate()
                {
                    if let Some(name) = cg_name {
                        let tgt_var_name = ld.var_names[name];
                        ld.vars_to_op_outputs_map[tgt_var_name] =
                            OpOutputIdx::from_usize(
                                sess.operator_bases[op_id]
                                    .outputs_start
                                    .into_usize()
                                    + cg_idx,
                            );
                    }
                }
            }
            OperatorData::NopCopy(_) => {
                output.flags.may_dup_or_drop = false;
                output.flags.non_stringified_input_access = false;
                ld.op_outputs[primary_output_idx]
                    .field_references
                    .push(input_field);
            }
            OperatorData::Format(fmt) => {
                update_op_format_variable_liveness(
                    sess,
                    fmt,
                    ld,
                    op_id,
                    op_offset_after_last_write,
                    output,
                );
            }
            OperatorData::Compute(c) => {
                update_op_compute_variable_liveness(
                    sess,
                    c,
                    ld,
                    op_id,
                    op_offset_after_last_write,
                    output,
                );
            }
            OperatorData::FileReader(_) => {
                // this only inserts if input is done, so no write flag
                // neccessary
                output.flags.input_accessed = false;
                output.flags.non_stringified_input_access = false;
            }
            OperatorData::Literal(di) => {
                output.flags.may_dup_or_drop = di.insert_count.is_some();
                output.flags.input_accessed = false;
                output.flags.non_stringified_input_access = false;
            }
            OperatorData::Join(_) => {}
            OperatorData::FieldValueSink(_) => {
                output.flags.may_dup_or_drop = false;
            }
            OperatorData::Custom(op) => Operator::update_variable_liveness(
                &**op,
                sess,
                ld,
                op_offset_after_last_write,
                op_id,
                bb_id,
                input_field,
                outputs_offset,
                output,
            ),
            OperatorData::MultiOp(op) => Operator::update_variable_liveness(
                op,
                sess,
                ld,
                op_offset_after_last_write,
                op_id,
                bb_id,
                input_field,
                outputs_offset,
                output,
            ),
            OperatorData::MacroCall(op) => {
                op.op_multi_op.update_variable_liveness(
                    sess,
                    ld,
                    op_offset_after_last_write,
                    op_id,
                    bb_id,
                    input_field,
                    outputs_offset,
                    output,
                )
            }
        }
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
            OperatorData::MultiOp(op) => {
                op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::MacroCall(op) => {
                op.op_multi_op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::Custom(op) => {
                op.on_liveness_computed(sess, ld, op_id)
            }
            OperatorData::Key(op) => {
                if let Some(NestedOp::SetUp(op_id)) = op.nested_op {
                    let op_data_id = sess.op_data_id(op_id);
                    let mut op_data =
                        std::mem::take(&mut sess.operator_data[op_data_id]);
                    op_data.on_liveness_computed(sess, ld, op_id);
                    sess.operator_data[op_data_id] = op_data;
                }
            }
            OperatorData::Transparent(op) => {
                if let NestedOp::SetUp(op_id) = op.nested_op {
                    let op_data_id = sess.op_data_id(op_id);
                    let mut op_data =
                        std::mem::take(&mut sess.operator_data[op_data_id]);
                    op_data.on_liveness_computed(sess, ld, op_id);
                    sess.operator_data[op_data_id] = op_data;
                }
            }
            OperatorData::Call(_)
            | OperatorData::Nop(_)
            | OperatorData::Atom(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::Compute(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::MacroDef(_) => (),
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
            OperatorData::Key(_)
            | OperatorData::MacroDef(_)
            | OperatorData::Atom(_)
            | OperatorData::Select(_) => unreachable!(),
            OperatorData::Nop(op) => build_tf_nop(op, tfs),
            OperatorData::Transparent(op) => {
                return build_tf_transparent(
                    op,
                    job,
                    tf_state,
                    op_id,
                    prebound_outputs,
                );
            }
            OperatorData::NopCopy(op) => build_tf_nop_copy(jd, op, tfs),
            OperatorData::Foreach(op) => {
                return insert_tf_foreach(
                    job,
                    op,
                    tf_state,
                    op_base.chain_id,
                    op_id,
                    prebound_outputs,
                );
            }
            OperatorData::ForeachUnique(op) => {
                return insert_tf_foreach_unique(
                    job,
                    op,
                    tf_state,
                    op_base.chain_id,
                    op_id,
                    prebound_outputs,
                );
            }
            OperatorData::Chunks(op) => {
                return insert_tf_chunks(
                    job,
                    op,
                    tf_state,
                    op_base.chain_id,
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
            OperatorData::Compute(op) => {
                build_tf_compute(jd, op_base, op, tfs)
            }
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
            OperatorData::Call(op) => build_tf_call(jd, op_base, op, tfs),
            OperatorData::CallConcurrent(op) => {
                build_tf_call_concurrent(jd, op_base, op, tfs)
            }

            OperatorData::Custom(op) => {
                match Operator::build_transforms(
                    &**op,
                    job,
                    tfs,
                    op_id,
                    prebound_outputs,
                ) {
                    TransformInstatiation::Single(tf) => tf,
                    TransformInstatiation::Multiple(instantiation) => {
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
                    TransformInstatiation::Single(tf) => tf,
                    TransformInstatiation::Multiple(instantiation) => {
                        return instantiation
                    }
                }
            }
            OperatorData::MacroCall(op) => {
                match op.op_multi_op.build_transforms(
                    job,
                    tfs,
                    op_id,
                    prebound_outputs,
                ) {
                    TransformInstatiation::Single(tf) => tf,
                    TransformInstatiation::Multiple(instantiation) => {
                        return instantiation
                    }
                }
            }
        };

        let next_input_field = tf_state.output_field;
        let next_group_track = tf_state.output_group_track_id;
        let next_match_set = tf_state.match_set_id;
        let tf_id = add_transform_to_job(
            &mut job.job_data,
            &mut job.transform_data,
            tf_state,
            data,
        );
        OperatorInstantiation {
            tfs_begin: tf_id,
            tfs_end: tf_id,
            next_match_set,
            next_input_field,
            next_group_track,
        }
    }

    pub fn aggregation_member(
        &self,
        agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        match self {
            OperatorData::MultiOp(mop) => {
                mop.sub_op_ids.get(agg_offset).copied()
            }
            OperatorData::MacroCall(mop) => {
                mop.op_multi_op.sub_op_ids.get(agg_offset).copied()
            }
            OperatorData::Key(op) => {
                if let Some(NestedOp::SetUp(op_id)) = op.nested_op {
                    return Some(op_id);
                }
                None
            }
            OperatorData::Transparent(op) => {
                if let NestedOp::SetUp(op_id) = op.nested_op {
                    return Some(op_id);
                }
                None
            }

            OperatorData::Custom(op) => op.aggregation_member(agg_offset),

            OperatorData::Nop(_)
            | OperatorData::NopCopy(_)
            | OperatorData::Atom(_)
            | OperatorData::Call(_)
            | OperatorData::CallConcurrent(_)
            | OperatorData::Print(_)
            | OperatorData::Join(_)
            | OperatorData::Fork(_)
            | OperatorData::ForkCat(_)
            | OperatorData::Select(_)
            | OperatorData::Regex(_)
            | OperatorData::Format(_)
            | OperatorData::Compute(_)
            | OperatorData::StringSink(_)
            | OperatorData::FieldValueSink(_)
            | OperatorData::FileReader(_)
            | OperatorData::Literal(_)
            | OperatorData::Foreach(_)
            | OperatorData::ForeachUnique(_)
            | OperatorData::Chunks(_)
            | OperatorData::MacroDef(_) => None,
        }
    }
}

pub enum TransformInstatiation<'a> {
    Single(TransformData<'a>),
    Multiple(OperatorInstantiation),
}

pub trait Operator: Send + Sync {
    fn default_name(&self) -> OperatorName;
    fn debug_op_name(&self) -> super::operator::OperatorName {
        self.default_name()
    }
    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> OutputFieldKind {
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
        _so: &mut SessionSetupData,
        _op_id: OperatorId,
        _add_to_chain: bool,
    ) {
    }
    fn on_subchains_added(&mut self, _curr_subchains_end: SubchainIndex) {}
    fn register_output_var_names(
        &self,
        _ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
    }
    fn update_bb_for_op(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_id: OperatorId,
        _op_n: OffsetInChain,
        _cn: &Chain,
        _bb_id: BasicBlockId,
    ) -> bool {
        false
    }
    fn assign_op_outputs(
        &mut self,
        _sess: &mut SessionData,
        _ld: &mut LivenessData,
        _op_id: OperatorId,
        _output_count: &mut OpOutputIdx,
    ) {
    }
    fn aggregation_member(
        &self,
        _agg_offset: OffsetInAggregation,
    ) -> Option<OperatorId> {
        None
    }

    // all of the &mut bool flags default to true
    // turning them to false allows for some pipeline optimizations
    // but may cause incorrect behavior if the promises made are broken later
    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        _outputs_offset: usize,
        _output: &mut OperatorLivenessOutput,
    ) {
    }
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
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
    ) -> TransformInstatiation<'a>;
}
