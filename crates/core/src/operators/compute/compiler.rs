use crate::{
    index_newtype,
    record_data::{
        field_value::FieldValue,
        object::{Object, ObjectKeysInterned},
    },
    utils::{
        index_slice::IndexSlice,
        index_vec::IndexVec,
        indexing_type::IndexingType,
        string_store::{StringStore, StringStoreEntry},
    },
};

use super::ast::{
    AccessIdx, BinaryOpKind, Block, BuiltinFunction, Expr, ExternIdentId,
    IdentId, LetBindingData, LetBindingId, UnaryOpKind, UnboundIdentData,
};

index_newtype! {
    pub struct InstructionId(u32);
    pub struct TempFieldIdRaw(u32);
    pub struct SsaTemporaryId(u32);
}

#[derive(Clone, Copy)]
pub struct TempFieldId {
    pub index: TempFieldIdRaw,
    pub generation: u32,
}

#[derive(Clone, Copy)]
pub struct TempFieldAccess {
    pub index: TempFieldIdRaw,
    pub access_index: AccessIdx,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct ExternIdentAccess {
    pub index: ExternIdentId,
    pub access_idx: AccessIdx,
}

#[derive(Clone)]
pub enum SsaValue {
    Unbound(ExternIdentId),
    Temporary(SsaTemporaryId),
    Literal(FieldValue),
}

#[derive(Clone)]
pub enum ValueAccess {
    Extern(ExternIdentAccess),
    TempField(TempFieldId),
    Literal(FieldValue),
}

struct IntermediateValue {
    value: SsaValue,
    release_after_use: bool,
}

#[derive(Clone, Copy)]
pub enum TargetRef {
    TempField(TempFieldIdRaw),
    Output,
    Discard,
}

pub enum Instruction {
    OpUnary {
        kind: UnaryOpKind,
        value: ValueAccess,
        target: TargetRef,
    },
    OpBinary {
        kind: BinaryOpKind,
        lhs: ValueAccess,
        rhs: ValueAccess,
        target: TargetRef,
    },
    CondCall {
        cond: ValueAccess,
        else_start: Option<InstructionId>,
        continuation: InstructionId,
    },
    ObjectKeysInterned {
        mappings: Box<[(StringStoreEntry, ValueAccess)]>,
        target: TargetRef,
    },
    ObjectKeysStored {
        mappings: Box<[(ValueAccess, ValueAccess)]>,
        target: TargetRef,
    },
    Array {
        elements: Box<[ValueAccess]>,
        target: TargetRef,
    },
    ArrayAccess {
        lhs: ValueAccess,
        index: ValueAccess,
        target: TargetRef,
    },
    DotAccess {
        lhs: ValueAccess,
        ident: StringStoreEntry,
        ident_str: String,
        target: TargetRef,
    },
    ClearTemporary(TempFieldId),
    Move {
        src: ValueAccess,
        tgt: TargetRef,
    },
    BuiltinFunction {
        kind: BuiltinFunction,
        args: Box<[ValueAccess]>,
        target: TargetRef,
    },
}

pub struct SsaTemporary {
    value: TempFieldId,
    access_count: AccessIdx,
    let_binding_count: u32,
}

pub struct Compiler<'a> {
    string_store: &'a mut StringStore,
    let_bindings: &'a IndexSlice<LetBindingId, LetBindingData>,
    unbound_idents: &'a mut IndexSlice<ExternIdentId, UnboundIdentData>,
    let_value_mappings: IndexVec<LetBindingId, SsaValue>,
    temporary_count: TempFieldIdRaw,
    unused_temporaries: Vec<TempFieldId>,
    ssa_temporaries: IndexVec<SsaTemporaryId, SsaTemporary>,
    instructions: IndexVec<InstructionId, Instruction>,
    // used for object and array to delay clear instructions
    pending_clear_instructions: Vec<TempFieldId>,
}

pub struct Compilation {
    pub instructions: IndexVec<InstructionId, Instruction>,
    pub temporary_slot_count: IndexVec<TempFieldIdRaw, AccessIdx>,
}

enum ObjectMappings {
    Literal(Object),
    KeysInterned(Vec<(StringStoreEntry, ValueAccess)>),
    KeysStored(Vec<(ValueAccess, ValueAccess)>),
}

impl SsaValue {
    fn take_value_accessed(
        &mut self,
        ssa_map: &mut IndexSlice<SsaTemporaryId, SsaTemporary>,
        unbound_idents: &mut IndexSlice<ExternIdentId, UnboundIdentData>,
    ) -> ValueAccess {
        match self {
            SsaValue::Unbound(ub_id) => {
                let ubd = &mut unbound_idents[*ub_id];
                let access_idx = ubd.access_count;
                ubd.access_count += AccessIdx::one();
                ValueAccess::Extern(ExternIdentAccess {
                    index: *ub_id,
                    access_idx,
                })
            }
            SsaValue::Temporary(ssa_tid) => {
                ssa_map[*ssa_tid].access_count += AccessIdx::one();
                ValueAccess::TempField(ssa_map[*ssa_tid].value)
            }
            SsaValue::Literal(v) => ValueAccess::Literal(std::mem::take(v)),
        }
    }
}

impl IntermediateValue {
    fn take_value_accessed(
        &mut self,
        ssa_map: &mut IndexSlice<SsaTemporaryId, SsaTemporary>,
        unbound_idents: &mut IndexSlice<ExternIdentId, UnboundIdentData>,
    ) -> ValueAccess {
        self.value.take_value_accessed(ssa_map, unbound_idents)
    }
    fn undef() -> Self {
        IntermediateValue {
            value: SsaValue::Literal(FieldValue::Undefined),
            release_after_use: false,
        }
    }
}

impl Compiler<'_> {
    fn claim_temporary_id(&mut self) -> TempFieldId {
        if let Some(unused) = self.unused_temporaries.pop() {
            return unused;
        }

        let idx_raw = self.temporary_count;
        self.temporary_count += TempFieldIdRaw::one();
        TempFieldId {
            index: idx_raw,
            generation: 0,
        }
    }
    fn release_temporary_id(&mut self, mut id: TempFieldId) {
        id.generation += 1;
        self.unused_temporaries.push(id);
    }
    fn claim_ssa_temporary(&mut self) -> (SsaTemporaryId, TempFieldId) {
        let temp_id = self.claim_temporary_id();
        let ssa_id = self.ssa_temporaries.push_get_id(SsaTemporary {
            value: temp_id,
            access_count: AccessIdx::ZERO,
            let_binding_count: 0,
        });
        (ssa_id, temp_id)
    }

    fn release_ssa_value_raw(&mut self, value: &SsaValue) {
        match value {
            SsaValue::Temporary(ssa_temp_id) => {
                let temporary_id = self.ssa_temporaries[*ssa_temp_id].value;
                self.release_temporary_id(temporary_id);
                self.instructions
                    .push(Instruction::ClearTemporary(temporary_id));
            }
            SsaValue::Unbound(_) | SsaValue::Literal(_) => {
                unreachable!()
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn release_intermediate(&mut self, v: IntermediateValue) {
        if v.release_after_use {
            self.release_ssa_value_raw(&v.value)
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn defer_release_intermediate(&mut self, v: IntermediateValue) {
        match v.value {
            SsaValue::Temporary(ssa_temp_id) => {
                if v.release_after_use {
                    self.pending_clear_instructions
                        .push(self.ssa_temporaries[ssa_temp_id].value);
                }
            }
            SsaValue::Unbound(_) | SsaValue::Literal(_) => {
                debug_assert!(!v.release_after_use);
            }
        }
    }
    fn emit_pending_clears(&mut self, retain: usize) {
        for mut temp_id in
            self.pending_clear_instructions[retain..].iter().copied()
        {
            self.instructions.push(Instruction::ClearTemporary(temp_id));
            temp_id.generation += 1;
            self.unused_temporaries.push(temp_id);
        }
        self.pending_clear_instructions.clear();
    }
    fn compile_block(&mut self, block: Block, target: TargetRef) {
        let stmt_count = block.stmts.len();

        for (i, expr) in block.stmts.into_vec().into_iter().enumerate() {
            let target = if i != stmt_count || block.trailing_semicolon {
                TargetRef::Discard
            } else {
                target
            };
            self.compile_expr_for_given_target(expr, target);
        }
    }
    fn reference_to_intermediate(
        &mut self,
        ident_id: IdentId,
        access_idx: AccessIdx,
    ) -> IntermediateValue {
        match ident_id {
            IdentId::LetBinding(lb_id) => {
                let last_access_through_let = access_idx + AccessIdx::one()
                    == self.let_bindings[lb_id].access_count;

                let value = self.let_value_mappings[lb_id].clone();

                let release_after_use = if last_access_through_let {
                    match &value {
                        SsaValue::Temporary(ssa_id) => {
                            let lbc = &mut self.ssa_temporaries[*ssa_id]
                                .let_binding_count;
                            *lbc -= 1;
                            *lbc == 0
                        }
                        SsaValue::Unbound(_) | SsaValue::Literal(_) => true,
                    }
                } else {
                    false
                };
                IntermediateValue {
                    value,
                    release_after_use,
                }
            }
            IdentId::Unbound(ub_id) => IntermediateValue {
                value: SsaValue::Unbound(ub_id),
                release_after_use: false,
            },
        }
    }
    fn compile_expr_for_temp_target(
        &mut self,
        expr: Expr,
    ) -> IntermediateValue {
        match expr {
            Expr::Literal(v) => IntermediateValue {
                value: SsaValue::Literal(v),
                release_after_use: false,
            },
            Expr::Reference {
                ident_id,
                access_idx: access_index,
            } => self.reference_to_intermediate(ident_id, access_index),
            Expr::OpUnary { kind, child } => {
                let mut subexpr_v = self.compile_expr_for_temp_target(*child);
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.instructions.push(Instruction::OpUnary {
                    kind,
                    value: subexpr_v.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    target: TargetRef::TempField(temp_id.index),
                });
                self.release_intermediate(subexpr_v);
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::DotAccess { lhs, ident } => {
                let mut lhs = self.compile_expr_for_temp_target(*lhs);
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.instructions.push(Instruction::DotAccess {
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    ident: self.string_store.intern_cloned(&ident),
                    ident_str: ident,
                    target: TargetRef::TempField(temp_id.index),
                });
                self.release_intermediate(lhs);
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::ArrayAccess { lhs, index } => {
                let mut lhs = self.compile_expr_for_temp_target(*lhs);
                let mut rhs = self.compile_expr_for_temp_target(*index);
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.instructions.push(Instruction::ArrayAccess {
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    index: rhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    target: TargetRef::TempField(temp_id.index),
                });
                self.release_intermediate(lhs);
                self.release_intermediate(rhs);
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::OpBinary { kind, children } => {
                let [lhs, rhs] = *children;
                let mut lhs = self.compile_expr_for_temp_target(lhs);
                let mut rhs = self.compile_expr_for_temp_target(rhs);
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.instructions.push(Instruction::OpBinary {
                    kind,
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    rhs: rhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    target: TargetRef::TempField(temp_id.index),
                });
                self.release_intermediate(lhs);
                self.release_intermediate(rhs);
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
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
                    let (ssa_id, temp_id) = self.claim_ssa_temporary();
                    let target = TargetRef::TempField(temp_id.index);
                    let result = IntermediateValue {
                        value: SsaValue::Temporary(ssa_id),
                        release_after_use: true,
                    };
                    (target, result)
                } else {
                    (TargetRef::Discard, IntermediateValue::undef())
                };
                self.compile_block(if_expr.then_block, target);

                let else_start = if let Some(else_block) = if_expr.else_block {
                    let start = self.instructions.next_idx();
                    self.compile_block(else_block, target);
                    Some(start)
                } else {
                    None
                };
                self.instructions.push(Instruction::CondCall {
                    cond: cond_v.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    else_start,
                    continuation: self.instructions.next_idx(),
                });
                self.release_intermediate(cond_v);
                result
            }
            Expr::Block(block) => {
                if block.trailing_semicolon {
                    let (ssa_id, temp_id) = self.claim_ssa_temporary();
                    self.compile_block(
                        block,
                        TargetRef::TempField(temp_id.index),
                    );
                    IntermediateValue {
                        value: SsaValue::Temporary(ssa_id),
                        release_after_use: true,
                    }
                } else {
                    self.compile_block(block, TargetRef::Discard);
                    IntermediateValue::undef()
                }
            }
            Expr::Object(o) => {
                let clears_before = self.pending_clear_count();
                let mappings = self.compile_object_mappings(o);
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                match mappings {
                    ObjectMappings::Literal(object) => {
                        self.emit_pending_clears(clears_before);
                        return IntermediateValue {
                            value: SsaValue::Literal(FieldValue::Object(
                                Box::new(object),
                            )),
                            release_after_use: false,
                        };
                    }
                    ObjectMappings::KeysInterned(mappings) => {
                        self.instructions.push(
                            Instruction::ObjectKeysInterned {
                                mappings: mappings.into_boxed_slice(),
                                target: TargetRef::TempField(temp_id.index),
                            },
                        );
                        self.emit_pending_clears(clears_before);
                    }
                    ObjectMappings::KeysStored(mappings) => {
                        self.instructions.push(
                            Instruction::ObjectKeysStored {
                                mappings: mappings.into_boxed_slice(),
                                target: TargetRef::TempField(temp_id.index),
                            },
                        );
                        self.emit_pending_clears(clears_before);
                    }
                }
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::Array(arr) => {
                let mut elements = Vec::new();
                let pending_clears_before = self.pending_clear_count();
                for e in arr {
                    let mut v = self.compile_expr_for_temp_target(e);
                    elements.push(v.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ));
                    self.defer_release_intermediate(v);
                }
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.instructions.push(Instruction::Array {
                    elements: elements.into_boxed_slice(),
                    target: TargetRef::TempField(temp_id.index),
                });
                self.emit_pending_clears(pending_clears_before);
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::FunctionCall { lhs, args } => {
                // TODO
                let (ssa_id, temp_id) = self.claim_ssa_temporary();
                self.compile_expr_for_given_target(
                    Expr::FunctionCall { lhs, args },
                    TargetRef::TempField(temp_id.index),
                );
                IntermediateValue {
                    value: SsaValue::Temporary(ssa_id),
                    release_after_use: true,
                }
            }
            Expr::BuiltinFunction(_) => {
                // would be handled by the parent FunctionCall node for now
                // as we don't have first class functions yet
                unreachable!()
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
                        .push(SsaValue::Literal(FieldValue::Undefined));
                } else {
                    let v = self.compile_expr_for_temp_target(*expr);
                    self.let_value_mappings.push(v.value);
                };
                IntermediateValue::undef()
            }
            Expr::Parentheses(expr) => {
                self.compile_expr_for_temp_target(*expr)
            }
        }
    }

    fn compile_object_mappings(
        &mut self,
        obj: Vec<(Expr, Option<Expr>)>,
    ) -> ObjectMappings {
        let mut all_keys_known = true;
        let mut all_values_known = true;
        // TODO: we could do some complex constant folding algorithm here
        // but for now this is all we are gonna do
        for (k, v) in &obj {
            all_keys_known &= matches!(k, Expr::Literal(FieldValue::Text(_)));
            all_values_known &= matches!(v, None | Some(Expr::Literal(_)))
        }

        if all_keys_known && all_values_known {
            let mut res = ObjectKeysInterned::new();
            for (key, value) in obj {
                let Expr::Literal(FieldValue::Text(s)) = key else {
                    unreachable!()
                };
                let key = self.string_store.intern_moved(s);
                let value = match value {
                    None => FieldValue::Undefined,
                    Some(Expr::Literal(value)) => value,
                    _ => unreachable!(),
                };
                res.insert(key, value);
            }
            return ObjectMappings::Literal(Object::KeysInterned(res));
        }

        if all_keys_known {
            let mut mappings = Vec::new();
            for (key, value) in obj {
                let Expr::Literal(FieldValue::Text(s)) = key else {
                    unreachable!()
                };
                let key = self.string_store.intern_moved(s);
                let mut value = if let Some(value) = value {
                    self.compile_expr_for_temp_target(value)
                } else {
                    IntermediateValue::undef()
                };
                mappings.push((
                    key,
                    value.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                ));
                self.defer_release_intermediate(value);
            }
            return ObjectMappings::KeysInterned(mappings);
        }

        let mut mappings = Vec::new();
        for (key, value) in obj {
            let mut key = self.compile_expr_for_temp_target(key);
            let key_v = key.take_value_accessed(
                &mut self.ssa_temporaries,
                self.unbound_idents,
            );
            self.defer_release_intermediate(key);
            let mut value = if let Some(value) = value {
                self.compile_expr_for_temp_target(value)
            } else {
                IntermediateValue::undef()
            };
            mappings.push((
                key_v,
                value.take_value_accessed(
                    &mut self.ssa_temporaries,
                    self.unbound_idents,
                ),
            ));
            self.defer_release_intermediate(value);
        }
        ObjectMappings::KeysStored(mappings)
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
                    src: ValueAccess::Literal(v),
                    tgt: target,
                })
            }
            Expr::Reference {
                ident_id,
                access_idx: access_index,
            } => {
                // this might drop the let binding so we have to do this
                // even if the intermediate is discarded
                let mut source =
                    self.reference_to_intermediate(ident_id, access_index);
                if discard {
                    return;
                }
                self.instructions.push(Instruction::Move {
                    src: source.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    tgt: target,
                });
                self.release_intermediate(source);
            }
            Expr::OpUnary { kind, child } => {
                let mut subexpr = self.compile_expr_for_temp_target(*child);
                self.instructions.push(Instruction::OpUnary {
                    kind,
                    value: subexpr.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    target,
                });
                self.release_intermediate(subexpr);
            }
            Expr::DotAccess { lhs, ident } => {
                let mut lhs = self.compile_expr_for_temp_target(*lhs);
                self.instructions.push(Instruction::DotAccess {
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    ident: self.string_store.intern_cloned(&ident),
                    ident_str: ident,
                    target,
                });
                self.release_intermediate(lhs);
            }
            Expr::ArrayAccess { lhs, index } => {
                let mut lhs = self.compile_expr_for_temp_target(*lhs);
                let mut index = self.compile_expr_for_temp_target(*index);
                self.instructions.push(Instruction::ArrayAccess {
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    index: index.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    target,
                });
                self.release_intermediate(lhs);
                self.release_intermediate(index);
            }
            Expr::OpBinary { kind, children } => {
                let [lhs, rhs] = *children;
                let mut lhs = self.compile_expr_for_temp_target(lhs);
                let mut rhs = self.compile_expr_for_temp_target(rhs);
                self.instructions.push(Instruction::OpBinary {
                    kind,
                    lhs: lhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    rhs: rhs.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
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
                let else_start = if let Some(else_block) = if_expr.else_block {
                    let start = self.instructions.next_idx();
                    self.compile_block(else_block, target);
                    Some(start)
                } else {
                    None
                };
                self.instructions.push(Instruction::CondCall {
                    cond: cond_v.take_value_accessed(
                        &mut self.ssa_temporaries,
                        self.unbound_idents,
                    ),
                    else_start,
                    continuation: self.instructions.next_idx(),
                });
                self.release_intermediate(cond_v);
            }
            Expr::Block(block) => {
                debug_assert!(discard || !block.trailing_semicolon);
                self.compile_block(block, target);
            }
            Expr::Object(obj) => {
                if discard {
                    for (key, value) in obj {
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
                    }
                    return;
                }
                let clear_count_before = self.pending_clear_count();
                let mappings = self.compile_object_mappings(obj);
                match mappings {
                    ObjectMappings::Literal(object) => {
                        self.instructions.push(Instruction::Move {
                            src: ValueAccess::Literal(FieldValue::Object(
                                Box::new(object),
                            )),
                            tgt: target,
                        });
                    }
                    ObjectMappings::KeysInterned(mappings) => self
                        .instructions
                        .push(Instruction::ObjectKeysInterned {
                            mappings: mappings.into_boxed_slice(),
                            target,
                        }),
                    ObjectMappings::KeysStored(mappings) => {
                        self.instructions.push(Instruction::ObjectKeysStored {
                            mappings: mappings.into_boxed_slice(),
                            target,
                        })
                    }
                }
                self.emit_pending_clears(clear_count_before);
            }
            Expr::Array(arr) => {
                let clear_count_before = self.pending_clear_count();
                let mut elements = Vec::new();
                for e in arr {
                    if discard {
                        self.compile_expr_for_given_target(
                            e,
                            TargetRef::Discard,
                        )
                    } else {
                        let mut v = self.compile_expr_for_temp_target(e);
                        elements.push(v.take_value_accessed(
                            &mut self.ssa_temporaries,
                            self.unbound_idents,
                        ));
                        self.defer_release_intermediate(v);
                    }
                }
                if !discard {
                    self.instructions.push(Instruction::Array {
                        elements: elements.into_boxed_slice(),
                        target,
                    });
                    self.emit_pending_clears(clear_count_before);
                }
            }
            Expr::FunctionCall { lhs, args } => {
                let Expr::BuiltinFunction(kind) = *lhs else {
                    unimplemented!("extern function calls");
                };
                let clear_count_before = self.pending_clear_count();
                let mut elements = Vec::new();
                for e in args {
                    if discard {
                        self.compile_expr_for_given_target(
                            e,
                            TargetRef::Discard,
                        )
                    } else {
                        let mut v = self.compile_expr_for_temp_target(e);
                        elements.push(v.take_value_accessed(
                            &mut self.ssa_temporaries,
                            self.unbound_idents,
                        ));
                        self.defer_release_intermediate(v);
                    }
                }
                // none of the builtin functions have side effects (yet)
                // if they ever do, we have to deal with that here and
                // don't discard the instruction
                if !discard {
                    self.instructions.push(Instruction::BuiltinFunction {
                        kind,
                        args: elements.into_boxed_slice(),
                        target,
                    });
                    self.emit_pending_clears(clear_count_before);
                }
            }
            Expr::BuiltinFunction(_) => {
                // should be handled by the parent FunctionCall node
                unreachable!()
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
                        .push(SsaValue::Literal(FieldValue::Undefined));
                } else {
                    let v = self.compile_expr_for_temp_target(*expr);
                    self.let_value_mappings.push(v.value);
                };
            }
            Expr::Parentheses(expr) => {
                self.compile_expr_for_given_target(*expr, target)
            }
        }
    }

    pub fn compile(
        stding_store: &mut StringStore,
        expr: Expr,
        let_bindings: &IndexSlice<LetBindingId, LetBindingData>,
        unbound_idents: &mut IndexSlice<ExternIdentId, UnboundIdentData>,
    ) -> Compilation {
        let mut compiler = Compiler {
            string_store: stding_store,
            let_bindings,
            unbound_idents,
            let_value_mappings: IndexVec::new(),
            temporary_count: TempFieldIdRaw::ZERO,
            unused_temporaries: Vec::new(),
            instructions: IndexVec::new(),
            pending_clear_instructions: Vec::new(),
            ssa_temporaries: IndexVec::new(),
        };
        compiler.compile_expr_for_given_target(expr, TargetRef::Output);
        let mut temporary_slot_count =
            IndexVec::from(vec![
                AccessIdx::ZERO;
                compiler.temporary_count.into_usize()
            ]);
        for ssa_val in compiler.ssa_temporaries {
            let sc = &mut temporary_slot_count[ssa_val.value.index];
            *sc = (*sc).max(ssa_val.access_count);
        }
        Compilation {
            instructions: compiler.instructions,
            temporary_slot_count,
        }
    }

    fn pending_clear_count(&self) -> usize {
        self.pending_clear_instructions.len()
    }
}
