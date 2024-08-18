use crate::{
    index_newtype,
    record_data::field_value::{FieldValue, Object},
    utils::{
        index_slice::IndexSlice, index_vec::IndexVec,
        indexing_type::IndexingType,
    },
};

use super::ast::{
    AccessIdx, BinaryOpKind, Block, Expr, IdentRefId, LetBindingData,
    LetBindingId, UnaryOpKind, UnboundRefData, UnboundRefId,
};

index_newtype! {
    pub struct InstructionId(u32);
    pub struct TemporaryIdxRaw(u32);
}

#[derive(Clone, Copy)]
struct TemporaryId {
    index: TemporaryIdxRaw,
    generation: u32,
}

#[derive(Clone)]
pub enum ValueRef {
    Unbound(UnboundRefId),
    Temporary(TemporaryId),
    Literal(FieldValue),
}

struct IntermediateValueRef {
    value: ValueRef,
    release_after_use: bool,
}

#[derive(Clone, Copy)]
pub enum TargetRef {
    Temporary(TemporaryId),
    Output,
    Discard,
}

pub enum Instruction {
    OpUnary {
        kind: UnaryOpKind,
        value: ValueRef,
        target: TargetRef,
    },
    OpBinary {
        kind: BinaryOpKind,
        lhs: ValueRef,
        rhs: ValueRef,
        target: TargetRef,
    },
    CondCall {
        cond: ValueRef,
        then_insn: InstructionId,
        else_insn: Option<InstructionId>,
    },
    Object {
        mappings: Box<[(ValueRef, Option<ValueRef>)]>,
        target: TargetRef,
    },
    Array {
        elements: Box<[ValueRef]>,
        target: TargetRef,
    },
    ClearTemporary(TemporaryId),
    Move {
        src: ValueRef,
        tgt: TargetRef,
    },
    Ret,
}

pub struct Compiler<'a> {
    unbound_refs: &'a IndexSlice<UnboundRefId, UnboundRefData>,
    let_bindings: &'a IndexSlice<LetBindingId, LetBindingData>,
    let_value_mappings: IndexVec<LetBindingId, ValueRef>,
    temporary_count: TemporaryIdxRaw,
    unused_temporaries: Vec<TemporaryId>,
    instructions: IndexVec<InstructionId, Instruction>,
    // used for object and array to delay clear instructions
    pending_clear_instructions: Vec<TemporaryId>,
}

pub struct Compilation {
    pub instructions: IndexVec<InstructionId, Instruction>,
    pub temporary_count: TemporaryIdxRaw,
}

impl IntermediateValueRef {
    fn take_value(&mut self) -> ValueRef {
        match &mut self.value {
            ValueRef::Unbound(ub_id) => ValueRef::Unbound(*ub_id),
            ValueRef::Temporary(tid) => ValueRef::Temporary(*tid),
            ValueRef::Literal(v) => ValueRef::Literal(std::mem::take(v)),
        }
    }
    fn undef() -> Self {
        IntermediateValueRef {
            value: ValueRef::Literal(FieldValue::Undefined),
            release_after_use: false,
        }
    }
}

impl Compiler<'_> {
    fn claim_temporary_id(&mut self) -> TemporaryId {
        if let Some(unused) = self.unused_temporaries.pop() {
            return unused;
        }

        let idx_raw = self.temporary_count;
        self.temporary_count += TemporaryIdxRaw::one();
        TemporaryId {
            index: idx_raw,
            generation: 0,
        }
    }
    fn release_temporary_id(&mut self, mut id: TemporaryId) {
        id.generation += 1;
        self.unused_temporaries.push(id);
    }
    fn release_intermediate(&mut self, v: IntermediateValueRef) {
        match v.value {
            ValueRef::Temporary(temp_id) => {
                if v.release_after_use {
                    self.release_temporary_id(temp_id);
                    self.instructions
                        .push(Instruction::ClearTemporary(temp_id));
                }
            }
            ValueRef::Unbound(_) | ValueRef::Literal(_) => {
                debug_assert!(!v.release_after_use);
            }
        }
    }
    fn defer_release_intermediate(&mut self, v: IntermediateValueRef) {
        match v.value {
            ValueRef::Temporary(temp_id) => {
                if v.release_after_use {
                    self.pending_clear_instructions.push(temp_id);
                }
            }
            ValueRef::Unbound(_) | ValueRef::Literal(_) => {
                debug_assert!(!v.release_after_use);
            }
        }
    }
    fn emit_pending_clears(&mut self) {
        for mut temp_id in self.pending_clear_instructions.iter().copied() {
            self.instructions.push(Instruction::ClearTemporary(temp_id));
            temp_id.generation += 1;
            self.unused_temporaries.push(temp_id);
        }
        self.pending_clear_instructions.clear();
    }
    fn compile_block(
        &mut self,
        block: Block,
        target: TargetRef,
    ) -> InstructionId {
        let first_insn = self.instructions.next_idx();
        let stmt_count = block.stmts.len();

        for (i, expr) in block.stmts.into_vec().into_iter().enumerate() {
            let target = if i != stmt_count || block.trailing_semicolon {
                TargetRef::Discard
            } else {
                target
            };
            self.compile_expr_for_given_target(expr, target);
        }
        self.instructions.push(Instruction::Ret);
        first_insn
    }
    fn compile_expr_for_temp_target(
        &mut self,
        expr: Expr,
    ) -> IntermediateValueRef {
        match expr {
            Expr::Literal(v) => IntermediateValueRef {
                value: ValueRef::Literal(v),
                release_after_use: false,
            },
            Expr::Reference { ref_id, access_idx } => match ref_id {
                IdentRefId::LetBinding(lb_id) => IntermediateValueRef {
                    value: self.let_value_mappings[lb_id].clone(),
                    release_after_use: access_idx + AccessIdx::one()
                        == self.let_bindings[lb_id].access_count,
                },
                IdentRefId::Unbound(ub_id) => IntermediateValueRef {
                    value: ValueRef::Unbound(ub_id),
                    release_after_use: false,
                },
            },
            Expr::OpUnary(kind, subexpr) => {
                let mut subexpr_v =
                    self.compile_expr_for_temp_target(*subexpr);
                let temp_id = self.claim_temporary_id();
                self.instructions.push(Instruction::OpUnary {
                    kind,
                    value: subexpr_v.take_value(),
                    target: TargetRef::Temporary(temp_id),
                });
                self.release_intermediate(subexpr_v);
                IntermediateValueRef {
                    value: ValueRef::Temporary(temp_id),
                    release_after_use: true,
                }
            }
            Expr::OpBinary(kind, sub_exprs) => {
                let [lhs, rhs] = *sub_exprs;
                let mut lhs = self.compile_expr_for_temp_target(lhs);
                let mut rhs = self.compile_expr_for_temp_target(rhs);
                let temp_id = self.claim_temporary_id();
                self.instructions.push(Instruction::OpBinary {
                    kind,
                    lhs: lhs.take_value(),
                    rhs: rhs.take_value(),
                    target: TargetRef::Temporary(temp_id),
                });
                self.release_intermediate(lhs);
                self.release_intermediate(rhs);
                IntermediateValueRef {
                    value: ValueRef::Temporary(temp_id),
                    release_after_use: true,
                }
            }
            Expr::IfExpr(if_expr) => {
                let mut cond_v =
                    self.compile_expr_for_temp_target(if_expr.cond);
                let has_result = !if_expr.then_block.trailing_semicolon;
                debug_assert_eq!(
                    has_result,
                    if_expr
                        .else_block
                        .as_ref()
                        .map(|b| !b.trailing_semicolon)
                        .unwrap_or(false)
                );
                let (target, result) = if has_result {
                    let temp_id = self.claim_temporary_id();
                    let target = TargetRef::Temporary(temp_id);
                    let result = IntermediateValueRef {
                        value: ValueRef::Temporary(temp_id),
                        release_after_use: true,
                    };
                    (target, result)
                } else {
                    (TargetRef::Discard, IntermediateValueRef::undef())
                };
                let then_insn = self.compile_block(if_expr.then_block, target);
                let else_insn =
                    if_expr.else_block.map(|b| self.compile_block(b, target));
                self.instructions.push(Instruction::CondCall {
                    cond: cond_v.take_value(),
                    then_insn,
                    else_insn,
                });
                self.release_intermediate(cond_v);
                result
            }
            Expr::Block(block) => {
                if block.trailing_semicolon {
                    let temp_id = self.claim_temporary_id();
                    self.compile_block(block, TargetRef::Temporary(temp_id));
                    IntermediateValueRef {
                        value: ValueRef::Temporary(temp_id),
                        release_after_use: true,
                    }
                } else {
                    self.compile_block(block, TargetRef::Discard);
                    IntermediateValueRef::undef()
                }
            }
            Expr::Object(o) => {
                let mut mappings = Vec::new();
                for (key, value) in o.into_vec() {
                    let mut key = self.compile_expr_for_temp_target(key);
                    let key_v = key.take_value();
                    self.defer_release_intermediate(key);
                    if let Some(value) = value {
                        let mut value =
                            self.compile_expr_for_temp_target(value);
                        mappings.push((key_v, Some(value.take_value())));
                        self.defer_release_intermediate(value);
                    } else {
                        mappings.push((key_v, None));
                    }
                }
                let temp_id = self.claim_temporary_id();
                self.instructions.push(Instruction::Object {
                    mappings: mappings.into_boxed_slice(),
                    target: TargetRef::Temporary(temp_id),
                });
                self.emit_pending_clears();
                IntermediateValueRef {
                    value: ValueRef::Temporary(temp_id),
                    release_after_use: true,
                }
            }
            Expr::Array(arr) => {
                let mut elements = Vec::new();
                for e in arr {
                    let mut v = self.compile_expr_for_temp_target(e);
                    elements.push(v.take_value());
                    self.defer_release_intermediate(v);
                }
                let temp_id = self.claim_temporary_id();
                self.instructions.push(Instruction::Array {
                    elements: elements.into_boxed_slice(),
                    target: TargetRef::Temporary(temp_id),
                });
                self.emit_pending_clears();
                IntermediateValueRef {
                    value: ValueRef::Temporary(temp_id),
                    release_after_use: true,
                }
            }
            Expr::FunctionCall(_, _) => {
                // TODO
                unimplemented!("function calls");
            }
            Expr::LetExpression(binding_id, expr) => {
                debug_assert_eq!(
                    self.let_value_mappings.next_idx(),
                    binding_id
                );
                if self.let_bindings[binding_id].access_count
                    == AccessIdx::ZERO
                {
                    self.compile_expr_for_given_target(
                        *expr,
                        TargetRef::Discard,
                    );
                    self.let_value_mappings
                        .push(ValueRef::Literal(FieldValue::Undefined));
                } else {
                    let mut v = self.compile_expr_for_temp_target(*expr);
                    self.let_value_mappings.push(v.take_value());
                    self.release_intermediate(v);
                };
                IntermediateValueRef::undef()
            }
        }
    }

    fn compile_expr_for_given_target(
        &mut self,
        expr: Expr,
        target: TargetRef,
    ) {
        let discard = matches!(target, TargetRef::Discard);
        match expr {
            Expr::Literal(v) => {
                if discard {
                    return;
                }
                self.instructions.push(Instruction::Move {
                    src: ValueRef::Literal(v),
                    tgt: target,
                })
            }
            Expr::Reference { ref_id, access_idx } => {
                let source = match ref_id {
                    IdentRefId::LetBinding(lb_id) => {
                        if access_idx + AccessIdx::one()
                            == self.let_bindings[lb_id].access_count
                        {
                            let source = std::mem::replace(
                                &mut self.let_value_mappings[lb_id],
                                ValueRef::Literal(FieldValue::Undefined),
                            );
                            if let ValueRef::Temporary(temp_id) = source {
                                self.release_temporary_id(temp_id);
                            }
                            source
                        } else {
                            self.let_value_mappings[lb_id].clone()
                        }
                    }
                    IdentRefId::Unbound(ub_id) => ValueRef::Unbound(ub_id),
                };
                if discard {
                    return;
                }
                self.instructions.push(Instruction::Move {
                    src: source,
                    tgt: target,
                });
            }
            Expr::OpUnary(kind, subexpr) => {
                let mut subexpr = self.compile_expr_for_temp_target(*subexpr);
                self.instructions.push(Instruction::OpUnary {
                    kind,
                    value: subexpr.take_value(),
                    target,
                });
                self.release_intermediate(subexpr);
            }
            Expr::OpBinary(kind, sub_exprs) => {
                let [lhs, rhs] = *sub_exprs;
                let mut lhs = self.compile_expr_for_temp_target(lhs);
                let mut rhs = self.compile_expr_for_temp_target(rhs);
                self.instructions.push(Instruction::OpBinary {
                    kind,
                    lhs: lhs.take_value(),
                    rhs: rhs.take_value(),
                    target,
                });
                self.release_intermediate(lhs);
                self.release_intermediate(rhs);
            }
            Expr::IfExpr(if_expr) => {
                let mut cond_v =
                    self.compile_expr_for_temp_target(if_expr.cond);
                if !discard {
                    debug_assert!(!if_expr.then_block.trailing_semicolon);
                    debug_assert!(
                        !if_expr
                            .else_block
                            .as_ref()
                            .unwrap()
                            .trailing_semicolon
                    );
                }
                let then_insn = self.compile_block(if_expr.then_block, target);
                let else_insn =
                    if_expr.else_block.map(|b| self.compile_block(b, target));
                self.instructions.push(Instruction::CondCall {
                    cond: cond_v.take_value(),
                    then_insn,
                    else_insn,
                });
                self.release_intermediate(cond_v);
            }
            Expr::Block(block) => {
                debug_assert!(discard || !block.trailing_semicolon);
                self.compile_block(block, target);
            }
            Expr::Object(o) => {
                let mut mappings = Vec::new();
                for (key, value) in o.into_vec() {
                    if discard {
                        self.compile_expr_for_given_target(
                            key,
                            TargetRef::Discard,
                        );
                        if let Some(value) = value {
                            self.compile_expr_for_given_target(
                                value,
                                TargetRef::Discard,
                            );
                        }
                    } else {
                        let mut key = self.compile_expr_for_temp_target(key);
                        let key_v = key.take_value();
                        self.defer_release_intermediate(key);
                        if let Some(value) = value {
                            let mut value =
                                self.compile_expr_for_temp_target(value);
                            mappings.push((key_v, Some(value.take_value())));
                            self.defer_release_intermediate(value);
                        } else {
                            mappings.push((key_v, None));
                        }
                    }
                }
                if !discard {
                    self.instructions.push(Instruction::Object {
                        mappings: mappings.into_boxed_slice(),
                        target,
                    });
                    self.emit_pending_clears();
                }
            }
            Expr::Array(arr) => {
                let mut elements = Vec::new();
                for e in arr {
                    if discard {
                        self.compile_expr_for_given_target(
                            e,
                            TargetRef::Discard,
                        )
                    } else {
                        let mut v = self.compile_expr_for_temp_target(e);
                        elements.push(v.take_value());
                        self.defer_release_intermediate(v);
                    }
                }
                if !discard {
                    self.instructions.push(Instruction::Array {
                        elements: elements.into_boxed_slice(),
                        target,
                    });
                    self.emit_pending_clears();
                }
            }
            Expr::FunctionCall(_, _) => {
                // TODO
                unimplemented!("function calls");
            }
            Expr::LetExpression(binding_id, expr) => {
                debug_assert!(discard);
                if self.let_bindings[binding_id].access_count
                    == AccessIdx::ZERO
                {
                    self.compile_expr_for_given_target(
                        *expr,
                        TargetRef::Discard,
                    );
                    self.let_value_mappings
                        .push(ValueRef::Literal(FieldValue::Undefined));
                } else {
                    let mut v = self.compile_expr_for_temp_target(*expr);
                    self.let_value_mappings.push(v.take_value());
                    self.release_intermediate(v);
                };
            }
        }
    }

    pub fn compile(
        expr: Expr,
        let_bindings: &IndexSlice<LetBindingId, LetBindingData>,
        unbound_refs: &IndexSlice<UnboundRefId, UnboundRefData>,
    ) -> Compilation {
        let mut compiler = Compiler {
            unbound_refs,
            let_bindings,
            let_value_mappings: IndexVec::new(),
            temporary_count: TemporaryIdxRaw::ZERO,
            unused_temporaries: Vec::new(),
            instructions: IndexVec::new(),
            pending_clear_instructions: Vec::new(),
        };
        compiler.compile_expr_for_given_target(expr, TargetRef::Output);
        Compilation {
            instructions: compiler.instructions,
            temporary_count: compiler.temporary_count,
        }
    }
}
