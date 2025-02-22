pub mod ast;
pub mod binary_op;
pub mod compiler;
pub mod executor;
pub mod executor_inserter;
pub mod lexer;
pub mod operations;
pub mod parser;
pub mod unary_op;

use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    index_newtype,
    job::JobData,
    liveness_analysis::{LivenessData, OperatorLivenessOutput},
    options::session_setup::SessionSetupData,
    record_data::{
        array::ArrayBuilder,
        field::{CowFieldDataRef, FieldIterRef},
        field_data::FieldData,
        field_data_ref::DestructuredFieldDataRef,
        field_value::FieldValue,
        iter::{field_iter::FieldIter, ref_iter::AutoDerefIter},
        iter_hall::{IterKind, IterStateRaw},
        object::{ObjectKeysInternedBuilder, ObjectKeysStoredBuilder},
        scope_manager::{Atom, ScopeValue},
        stream_value::StreamValueUpdate,
    },
    typeline_error::TypelineError,
    utils::{
        index_slice::IndexSlice,
        index_vec::IndexVec,
        indexing_type::IndexingType,
        phantom_slot::PhantomSlot,
        stable_universe::StableUniverse,
        temp_vec::{TempVec, TransmutableContainer},
    },
};
use ast::{
    AccessIdx, Expr, ExternIdentId, LetBindingData, LetBindingId,
    UnboundIdentData,
};
use compiler::{Compilation, Compiler, InstructionId, TempFieldIdRaw};
use executor::{Executor, ExecutorInputIter, ExternFieldTempIterId};
use lexer::ComputeExprLexer;
use parser::ComputeExprParser;

use super::{
    errors::OperatorCreationError,
    operator::{
        OffsetInChain, Operator, OperatorDataId, OperatorId,
        OperatorOffsetInChain, TransformInstatiation,
    },
    transform::{Transform, TransformId, TransformState},
};

// we compile during setup because we need e.g. the string store
pub enum ComputeExpression {
    Ast(ast::Expr),
    Compiled(Compilation),
}

pub struct OpCompute {
    #[cfg(feature = "debug_state")]
    expr_str: String,
    expr: ComputeExpression,
    unbound_refs: IndexVec<ExternIdentId, UnboundIdentData>,
    let_bindings: IndexVec<LetBindingId, LetBindingData>,
}

index_newtype! {
    pub struct ExternFieldIdx(u32);
}

pub struct ExternField {
    iter_ref: FieldIterRef,
    iter_slots: Box<IndexSlice<AccessIdx, Option<ExternFieldTempIterId>>>,
}

pub enum ExternVarData {
    Atom(Arc<Atom>),
    Field(ExternFieldIdx),
    Literal(FieldValue),
}

pub struct TempField {
    pub field_pos: Cell<usize>,
    pub data: RefCell<FieldData>,
    pub iter_slots: Box<IndexSlice<AccessIdx, IterStateRaw>>,
}

pub struct TfCompute<'a> {
    op: &'a OpCompute,
    temp_fields: Box<IndexSlice<TempFieldIdRaw, TempField>>,
    extern_vars: IndexVec<ExternIdentId, ExternVarData>,
    extern_fields: IndexVec<ExternFieldIdx, ExternField>,
    extern_field_refs:
        IndexVec<ExternFieldIdx, PhantomSlot<CowFieldDataRef<'static>>>,
    extern_field_iters: IndexVec<
        ExternFieldIdx,
        PhantomSlot<
            AutoDerefIter<
                'static,
                FieldIter<DestructuredFieldDataRef<'static>>,
            >,
        >,
    >,
    extern_field_temp_iters: StableUniverse<
        ExternFieldTempIterId,
        PhantomSlot<
            RefCell<
                AutoDerefIter<
                    'static,
                    FieldIter<DestructuredFieldDataRef<'static>>,
                >,
            >,
        >,
    >,
    executor_iters_temp: TempVec<ExecutorInputIter<'static, 'static>>,
    array_builder: ArrayBuilder,
    object_keys_interned_builder: ObjectKeysInternedBuilder,
    object_keys_stored_builder: ObjectKeysStoredBuilder,
}

pub fn build_op_compute(
    fmt: &[u8],
    span: Span,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let mut let_bindings = IndexVec::new();
    let mut unbound_refs = IndexVec::new();

    let mut p = ComputeExprParser::new(
        ComputeExprLexer::new(fmt),
        &mut unbound_refs,
        &mut let_bindings,
    );

    let expr = p.parse().map_err(|e| {
        OperatorCreationError::new_s(e.stringify_error("<expr>"), span)
    })?;

    #[cfg(feature = "debug_state")]
    use crate::utils::escaped_writer::{
        escape_to_string, ESCAPE_DOUBLE_QUOTES,
    };

    Ok(Box::new(OpCompute {
        #[cfg(feature = "debug_state")]
        expr_str: escape_to_string(fmt, &ESCAPE_DOUBLE_QUOTES),
        expr: ComputeExpression::Ast(expr),
        unbound_refs,
        let_bindings,
    }))
}

impl Operator for OpCompute {
    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        for r in &mut self.unbound_refs {
            r.name_interned = sess.string_store.intern_cloned(&r.name);
        }
        let ComputeExpression::Ast(expr) = std::mem::replace(
            &mut self.expr,
            ComputeExpression::Ast(Expr::Literal(FieldValue::Undefined)),
        ) else {
            unreachable!()
        };

        let compilation = Compiler::compile(
            &mut sess.string_store,
            expr,
            &self.let_bindings,
            &mut self.unbound_refs,
        );

        self.expr = ComputeExpression::Compiled(compilation);

        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
    }
    fn default_name(&self) -> super::operator::OperatorName {
        "c".into()
    }
    fn debug_op_name(&self) -> super::operator::OperatorName {
        #[cfg(feature = "debug_state")]
        return format!("c=\"{}\"", self.expr_str).into();

        #[cfg(not(feature = "debug_state"))]
        "c".into()
    }

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
        for r in &self.unbound_refs {
            if r.name != "_" {
                ld.add_var_name(r.name_interned);
            }
        }
    }

    fn update_variable_liveness(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        _bb_id: crate::liveness_analysis::BasicBlockId,
        _input_field: crate::liveness_analysis::OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        output.flags.input_accessed = false;
        for ir in &self.unbound_refs {
            if ir.name == "_" {
                output.flags.input_accessed = true;
                continue;
            };
            ld.access_var(
                sess,
                op_id,
                ld.var_names[&ir.name_interned],
                op_offset_after_last_write,
                true,
            );
        }
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut crate::job::Job<'a>,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &super::operator::PreboundOutputsMap,
    ) -> super::operator::TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let mut idents = IndexVec::new();
        let mut unbound_fields = IndexVec::new();

        let next_actor_id = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow()
            .peek_next_actor_id();

        let scope_id =
            jd.match_set_mgr.match_sets[tf_state.match_set_id].active_scope;

        for key_ref in &self.unbound_refs {
            let field_id = if &key_ref.name == "_" {
                let mut f =
                    jd.field_mgr.fields[tf_state.input_field].borrow_mut();
                // while the ref count was already bumped by the transform
                // creation cleaning up this transform is
                // simpler this way
                f.ref_count += 1;
                tf_state.input_field
            } else if let Some(val) =
                jd.scope_mgr.lookup_value(scope_id, key_ref.name_interned)
            {
                match val {
                    ScopeValue::Atom(atom) => {
                        idents.push(ExternVarData::Atom(atom.clone()));
                        continue;
                    }
                    &ScopeValue::Field(field_id) => {
                        jd.field_mgr
                            .setup_field_refs(&mut jd.match_set_mgr, field_id);
                        let mut f = jd.field_mgr.fields[field_id].borrow_mut();
                        f.ref_count += 1;
                        field_id
                    }
                    ScopeValue::OpDecl(m) => {
                        idents.push(ExternVarData::Literal(
                            FieldValue::OpDecl(m.clone()),
                        ));
                        continue;
                    }
                }
            } else {
                let dummy_field =
                    jd.match_set_mgr.get_dummy_field(tf_state.match_set_id);
                jd.scope_mgr.insert_field_name(
                    scope_id,
                    key_ref.name_interned,
                    dummy_field,
                );
                dummy_field
            };
            let field_idx = unbound_fields.push_get_id(ExternField {
                iter_ref: FieldIterRef {
                    field_id,
                    iter_id: jd.field_mgr.claim_iter(
                        field_id,
                        next_actor_id,
                        IterKind::Transform(
                            jd.tf_mgr.transforms.peek_claim_id(),
                        ),
                    ),
                },
                iter_slots: IndexSlice::from_boxed_slice(
                    vec![None; key_ref.access_count.into_usize()]
                        .into_boxed_slice(),
                ),
            });
            idents.push(ExternVarData::Field(field_idx));
        }
        let mut temporaries = IndexVec::new();

        let ComputeExpression::Compiled(compilation) = &self.expr else {
            unreachable!()
        };

        for &slot_count in &compilation.temporary_slot_count {
            temporaries.push(TempField {
                data: RefCell::new(FieldData::default()),
                field_pos: Cell::new(usize::MAX),
                iter_slots: IndexSlice::from_boxed_slice(
                    vec![IterStateRaw::default(); slot_count.into_usize()]
                        .into_boxed_slice(),
                ),
            });
        }

        let tf = TfCompute {
            op: self,
            extern_vars: idents,
            temp_fields: temporaries.into_boxed_slice(),
            extern_field_refs: IndexVec::with_capacity(unbound_fields.len()),
            extern_field_iters: IndexVec::with_capacity(unbound_fields.len()),
            extern_field_temp_iters: StableUniverse::default(),
            extern_fields: unbound_fields,
            executor_iters_temp: TempVec::default(),
            array_builder: ArrayBuilder::default(),
            object_keys_interned_builder: ObjectKeysInternedBuilder::default(),
            object_keys_stored_builder: ObjectKeysStoredBuilder::default(),
        };
        TransformInstatiation::Single(Box::new(tf))
    }
}
impl<'a> Transform<'a> for TfCompute<'a> {
    fn update(&mut self, jd: &mut JobData<'a>, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let op_id = tf.op_id.unwrap();
        let of_id = tf.output_field;
        jd.tf_mgr.prepare_output_field(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
        );
        let mut extern_field_refs = self.extern_field_refs.take_transmute();
        {
            let mut extern_field_iters =
                self.extern_field_iters.take_transmute();
            let mut extern_field_temp_iters =
                self.extern_field_temp_iters.take_transmute();

            for uf in &self.extern_fields {
                extern_field_refs.push(jd.field_mgr.get_cow_field_ref(
                    &jd.match_set_mgr,
                    uf.iter_ref.field_id,
                ));
            }
            for (uf_id, fr) in extern_field_refs.iter_enumerated() {
                extern_field_iters.push(jd.field_mgr.get_auto_deref_iter(
                    self.extern_fields[uf_id].iter_ref.field_id,
                    fr,
                    self.extern_fields[uf_id].iter_ref.iter_id,
                ))
            }

            let mut output = jd.field_mgr.fields[of_id].borrow_mut();
            let field_pos = output.iter_hall.get_field_count(&jd.field_mgr);

            let ComputeExpression::Compiled(compilation) = &self.op.expr
            else {
                unreachable!()
            };

            let mut exec = Executor {
                op_id,
                fm: &jd.field_mgr,
                msm: &jd.match_set_mgr,
                compilation,
                extern_field_iters: &mut extern_field_iters,
                output: &mut output.iter_hall,
                temp_fields: &mut self.temp_fields,
                extern_vars: &mut self.extern_vars,
                extern_fields: &mut self.extern_fields,
                extern_field_temp_iters: &mut extern_field_temp_iters,
                executor_iters_temp: &mut self.executor_iters_temp,
                array_builder: &mut self.array_builder,
                object_keys_interned_builder: &mut self
                    .object_keys_interned_builder,
                object_keys_stored_builder: &mut self
                    .object_keys_stored_builder,
            };
            exec.run(
                InstructionId::ZERO..compilation.instructions.next_idx(),
                field_pos,
                batch_size,
            );

            for ef in self.extern_fields.iter_mut().rev() {
                let mut iter = None;
                for slot in &mut *ef.iter_slots {
                    if let &mut Some(idx) = slot {
                        if iter.is_none() {
                            iter = Some(
                                extern_field_temp_iters
                                    .release(idx)
                                    .into_inner(),
                            );
                        }
                        *slot = None;
                    }
                }
                if iter.is_none() {
                    iter = extern_field_iters.pop();
                } else {
                    extern_field_iters.pop();
                }
                let mut iter = iter.unwrap();
                let rem = batch_size - (iter.get_next_field_pos() - field_pos);
                iter.next_n_fields(rem);
                jd.field_mgr.store_iter_from_ref(ef.iter_ref, iter);
            }

            self.extern_field_iters.reclaim_temp(extern_field_iters);
            self.extern_field_temp_iters
                .reclaim_temp_take(&mut extern_field_temp_iters);
        }

        self.extern_field_refs.reclaim_temp(extern_field_refs);

        jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
    }

    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData<'a>,
        _svu: StreamValueUpdate,
    ) {
        todo!()
    }
}

pub fn parse_op_compute(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let val = expr.require_single_plaintext_arg_autoconvert(sess)?;
    build_op_compute(val.as_bytes(), expr.span)
}
pub fn create_op_compute(
    val: &str,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    build_op_compute(val.as_bytes(), Span::Generated)
}
pub fn create_op_compute_b(
    val: &[u8],
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    build_op_compute(val, Span::Generated)
}

pub fn build_op_to_int(span: Span) -> Box<dyn Operator> {
    build_op_compute(b"int(_)", span).unwrap()
}

pub fn create_op_to_int() -> Box<dyn Operator> {
    build_op_to_int(Span::Generated)
}

pub fn build_op_to_float(span: Span) -> Box<dyn Operator> {
    build_op_compute(b"float(_)", span).unwrap()
}

pub fn create_op_to_float() -> Box<dyn Operator> {
    build_op_to_float(Span::Generated)
}
