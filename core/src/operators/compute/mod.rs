pub mod ast;
pub mod compiler;
pub mod executor;
pub mod lexer;
pub mod parser;

use std::{mem::size_of, sync::Arc};

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    index_newtype,
    job::JobData,
    liveness_analysis::{AccessFlags, LivenessData},
    options::session_setup::SessionSetupData,
    record_data::{
        field::{CowFieldDataRef, FieldIterRef},
        field_data::FieldData,
        field_value::FieldValue,
        iter_hall::{IterKind, IterStateRaw},
        iters::{DestructuredFieldDataRef, FieldIter},
        ref_iter::AutoDerefIter,
        scope_manager::{Atom, ScopeValue},
        stream_value::StreamValueUpdate,
    },
    scr_error::ScrError,
    utils::{
        index_slice::IndexSlice,
        index_vec::IndexVec,
        indexing_type::IndexingType,
        phantom_slot::PhantomSlot,
        temp_vec::TransmutableContainer,
        universe::{Universe, UniverseEntry},
    },
};
use ast::{AccessIdx, ExternIdentId, UnboundIdentData};
use compiler::{Compilation, Compiler, InstructionId, TempFieldIdRaw};
use executor::{Exectutor, ExternFieldTempIterId};
use lexer::ComputeExprLexer;
use parser::ComputeExprParser;

use super::{
    errors::OperatorCreationError,
    operator::{
        OffsetInChain, OperatorBase, OperatorData, OperatorDataId, OperatorId,
        OperatorOffsetInChain,
    },
    transform::{TransformData, TransformId, TransformState},
};

pub struct OpCompute {
    unbound_refs: IndexVec<ExternIdentId, UnboundIdentData>,
    compilation: Compilation,
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
    pub field_pos: usize,
    pub data: FieldData,
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
                FieldIter<'static, DestructuredFieldDataRef<'static>>,
            >,
        >,
    >,
    extern_field_temp_iters: Universe<
        ExternFieldTempIterId,
        PhantomSlot<
            AutoDerefIter<
                'static,
                FieldIter<'static, DestructuredFieldDataRef<'static>>,
            >,
        >,
    >,
}

pub fn setup_op_compute(
    op: &mut OpCompute,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    for r in &mut op.unbound_refs {
        r.name_interned = sess.string_store.intern_cloned(&r.name);
    }
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
}

pub fn compute_add_var_names(c: &OpCompute, ld: &mut LivenessData) {
    for r in &c.unbound_refs {
        if r.name != "_" {
            ld.add_var_name(r.name_interned);
        }
    }
}

pub fn update_op_compute_variable_liveness(
    sess: &SessionData,
    c: &OpCompute,
    ld: &mut LivenessData,
    op_id: OperatorId,
    access_flags: &mut AccessFlags,
    op_offset_after_last_write: OffsetInChain,
) {
    access_flags.may_dup_or_drop = false;
    access_flags.input_accessed = false;
    for ir in &c.unbound_refs {
        if ir.name == "_" {
            access_flags.input_accessed = true;
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

pub fn build_op_compute(
    fmt: &[u8],
    span: Span,
) -> Result<OperatorData, OperatorCreationError> {
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

    let compilation =
        Compiler::compile(expr, &let_bindings, &mut unbound_refs);

    Ok(OperatorData::Compute(OpCompute {
        unbound_refs,
        compilation,
    }))
}

pub fn build_tf_compute<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpCompute,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let mut idents = IndexVec::new();
    let mut unbound_fields = IndexVec::new();

    let scope_id =
        jd.match_set_mgr.match_sets[tf_state.match_set_id].active_scope;

    for key_ref in &op.unbound_refs {
        let field_id = if &key_ref.name == "_" {
            let mut f = jd.field_mgr.fields[tf_state.input_field].borrow_mut();
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
                ScopeValue::Macro(m) => {
                    idents.push(ExternVarData::Literal(FieldValue::Macro(
                        m.clone(),
                    )));
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
                iter_id: jd.field_mgr.claim_iter_non_cow(
                    field_id,
                    IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
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
    for &slot_count in &op.compilation.temporary_slot_count {
        temporaries.push(TempField {
            data: FieldData::default(),
            field_pos: usize::MAX,
            iter_slots: IndexSlice::from_boxed_slice(
                vec![IterStateRaw::default(); slot_count.into_usize()]
                    .into_boxed_slice(),
            ),
        });
    }

    let tf = TfCompute {
        op,
        extern_vars: idents,
        temp_fields: temporaries.into_boxed_slice(),
        extern_field_refs: IndexVec::with_capacity(unbound_fields.len()),
        extern_field_iters: IndexVec::with_capacity(unbound_fields.len()),
        extern_field_temp_iters: Universe::default(),
        extern_fields: unbound_fields,
    };
    TransformData::Compute(tf)
}

pub fn handle_tf_compute(
    jd: &mut JobData,
    tf_id: TransformId,
    c: &mut TfCompute,
) {
    let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
    let tf = &jd.tf_mgr.transforms[tf_id];
    let op_id = tf.op_id.unwrap();
    let of_id = tf.output_field;
    jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );
    let mut extern_field_refs = c.extern_field_refs.take_transmute();
    let mut extern_field_iters = c.extern_field_iters.take_transmute();
    assert_eq!(
        size_of::<
            UniverseEntry<
                PhantomSlot<
                    AutoDerefIter<
                        'static,
                        FieldIter<'static, DestructuredFieldDataRef<'static>>,
                    >,
                >,
            >,
        >(),
        size_of::<
            UniverseEntry<
                AutoDerefIter<
                    'static,
                    FieldIter<'static, DestructuredFieldDataRef<'static>>,
                >,
            >,
        >()
    );
    let extern_field_temp_iters = c.extern_field_temp_iters.take_transmute();
    for uf in &c.extern_fields {
        extern_field_refs.push(
            jd.field_mgr
                .get_cow_field_ref(&jd.match_set_mgr, uf.iter_ref.field_id),
        );
    }
    for (uf_id, fr) in extern_field_refs.iter_enumerated() {
        extern_field_iters.push(jd.field_mgr.get_auto_deref_iter(
            c.extern_fields[uf_id].iter_ref.field_id,
            fr,
            c.extern_fields[uf_id].iter_ref.iter_id,
        ))
    }

    let mut output = jd.field_mgr.fields[of_id].borrow_mut();
    let field_pos = output.iter_hall.get_field_count(&jd.field_mgr);
    let mut exec = Exectutor {
        op_id,
        fm: &jd.field_mgr,
        msm: &jd.match_set_mgr,
        compilation: &c.op.compilation,
        extern_field_iters: &mut extern_field_iters,
        output: &mut output.iter_hall,
        extern_field_temp_iters,
        temp_fields: &mut c.temp_fields,
        extern_vars: &mut c.extern_vars,
        extern_fields: &mut c.extern_fields,
    };
    exec.handle_batch(
        InstructionId::ZERO..c.op.compilation.instructions.next_idx(),
        field_pos,
        batch_size,
    );
    c.extern_field_temp_iters
        .reclaim_temp_take(&mut exec.extern_field_temp_iters);
    drop(exec);
    while let Some(iter) = extern_field_iters.pop() {
        jd.field_mgr.store_iter_from_ref(
            c.extern_fields[extern_field_iters.next_idx()].iter_ref,
            iter,
        );
    }
    c.extern_field_iters.reclaim_temp(extern_field_iters);
    c.extern_field_refs.reclaim_temp(extern_field_refs);

    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
}

pub fn handle_tf_compute_stream_value_update<'a>(
    _jd: &mut JobData<'a>,
    _fmt: &mut TfCompute<'a>,
    _update: StreamValueUpdate,
) {
    todo!()
}

pub fn parse_op_compute(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let val = expr.require_single_plaintext_arg_autoconvert(sess)?;
    build_op_compute(val.as_bytes(), expr.span)
}
pub fn create_op_compute(
    val: &str,
) -> Result<OperatorData, OperatorCreationError> {
    build_op_compute(val.as_bytes(), Span::Generated)
}
pub fn create_op_compute_b(
    val: &[u8],
) -> Result<OperatorData, OperatorCreationError> {
    build_op_compute(val, Span::Generated)
}
