pub mod ast;
mod lexer;
pub mod parser;

use std::sync::Arc;

use crate::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    job::JobData,
    liveness_analysis::{AccessFlags, LivenessData},
    options::session_setup::SessionSetupData,
    record_data::{
        field::FieldIterRef, iter_hall::IterKind, scope_manager::Atom,
        stream_value::StreamValueUpdate,
    },
    scr_error::ScrError,
    utils::index_vec::IndexVec,
};
use ast::{
    ComputeIdentRefData, ComputeTemporaryRefData, ComputeValueRefType, Expr,
    TemporaryRefId, UnboundRefId,
};
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
    expr: Expr,
    ident_refs: IndexVec<UnboundRefId, ComputeIdentRefData>,
    temporaries: IndexVec<TemporaryRefId, ComputeTemporaryRefData>,
}

#[derive(Clone)]
pub enum ComputeVarRef {
    Atom(Arc<Atom>),
    Field(FieldIterRef),
}

pub struct TfCompute<'a> {
    op: &'a OpCompute,
    idents: IndexVec<UnboundRefId, ComputeVarRef>,
}

pub fn setup_op_compute(
    op: &mut OpCompute,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    for r in &mut op.ident_refs {
        r.name_interned = sess.string_store.intern_cloned(&r.name);
    }
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
}

pub fn compute_add_var_names(c: &OpCompute, ld: &mut LivenessData) {
    for r in &c.ident_refs {
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
    // might be set to true again in the loop below
    access_flags.non_stringified_input_access = false;
    access_flags.input_accessed = false;
    for ir in &c.ident_refs {
        if ir.name == "_" {
            access_flags.input_accessed = true;
            access_flags.non_stringified_input_access = true;
            continue;
        };
        ld.access_var(
            sess,
            op_id,
            ld.var_names[&ir.name_interned],
            op_offset_after_last_write,
            false,
        );
    }
}

pub fn build_op_compute(
    fmt: &[u8],
    span: Span,
) -> Result<OperatorData, OperatorCreationError> {
    let mut ident_refs = IndexVec::new();
    let mut temporaries = IndexVec::new();

    let mut p = ComputeExprParser::new(
        ComputeExprLexer::new(fmt),
        &mut ident_refs,
        &mut temporaries,
    );
    let expr = p.parse().map_err(|e| {
        OperatorCreationError::new_s(e.stringify_error("<expr>"), span)
    })?;

    Ok(OperatorData::Compute(OpCompute {
        expr,
        ident_refs,
        temporaries,
    }))
}

pub fn build_tf_compute<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpCompute,
    tf_state: &TransformState,
) -> TransformData<'a> {
    let mut idents = IndexVec::new();

    let scope_id =
        jd.match_set_mgr.match_sets[tf_state.match_set_id].active_scope;

    for key_ref in &op.ident_refs {
        match key_ref.ref_type {
            ComputeValueRefType::Atom => {
                let atom =
                    jd.scope_mgr.lookup_atom(scope_id, key_ref.name_interned);
                let atom = match atom {
                    Some(v) => v.clone(),
                    None => todo!(),
                };
                idents.push(ComputeVarRef::Atom(atom));
                continue;
            }
            ComputeValueRefType::Field => (),
        };

        let field_id = if &key_ref.name != "_" {
            if let Some(id) =
                jd.scope_mgr.lookup_field(scope_id, key_ref.name_interned)
            {
                jd.field_mgr.setup_field_refs(&mut jd.match_set_mgr, id);
                let mut f = jd.field_mgr.fields[id].borrow_mut();
                f.ref_count += 1;
                id
            } else {
                let dummy_field =
                    jd.match_set_mgr.get_dummy_field(tf_state.match_set_id);
                jd.scope_mgr.insert_field_name(
                    scope_id,
                    key_ref.name_interned,
                    dummy_field,
                );
                dummy_field
            }
        } else {
            let mut f = jd.field_mgr.fields[tf_state.input_field].borrow_mut();
            // while the ref count was already bumped by the transform
            // creation cleaning up this transform is
            // simpler this way
            f.ref_count += 1;
            tf_state.input_field
        };
        idents.push(ComputeVarRef::Field(FieldIterRef {
            field_id,
            iter_id: jd.field_mgr.claim_iter_non_cow(
                field_id,
                IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
            ),
        }))
    }

    let tf = TfCompute { idents, op };
    TransformData::Compute(tf)
}

pub fn handle_tf_compute(
    jd: &mut JobData,
    tf_id: TransformId,
    c: &mut TfCompute,
) {
    todo!()
}

pub fn handle_tf_compute_stream_value_update<'a>(
    _jd: &mut JobData<'a>,
    _fmt: &mut TfCompute<'a>,
    _update: StreamValueUpdate,
) {
    todo!()
}

pub fn parse_op_compute(
    expr: &CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    let val = expr.require_single_plaintext_arg()?;
    build_op_compute(val, expr.span)
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
