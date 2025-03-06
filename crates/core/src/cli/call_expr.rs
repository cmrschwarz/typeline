use std::{borrow::Cow, fmt::Debug};

use bstr::ByteSlice;
use indexland::{Idx, NonMax};
use num::PrimInt;

use crate::{
    operators::{errors::OperatorCreationError, operator::OperatorId},
    options::{
        chain_settings::RationalsPrintMode, session_setup::SessionSetupData,
    },
    record_data::{
        array::Array,
        field_value::{FieldValue, FieldValueKind},
        formattable::{Formattable, FormattingContext, RealizedFormatKey},
        object::{Object, ObjectKeysStored},
        scope_manager::ScopeId,
    },
    utils::{
        lazy_lock_guard::{LazyRwLockGuard, LazyRwLockWriteGuard},
        maybe_text::{MaybeText, MaybeTextCow},
        string_store::StringStore,
    },
};

use super::{try_parse_bool, CliArgumentError};

pub type CliArgIdx = NonMax<u32>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Span {
    #[default]
    Builtin,
    Generated,
    FlagsObject,
    CliArg {
        start: CliArgIdx,
        end: CliArgIdx,
        offset_start: u16,
        offset_end: u16,
    },
    MacroExpansion {
        op_id: OperatorId,
        // TODO: do some interned way to refer to expanded macro location
    },
    EnvVar {
        compile_time: bool,
        var_name: &'static str,
    },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MetaInfo {
    EndKind(CallExprEndKind),
    DenormalRepresentation(Box<str>),
}

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Argument {
    pub value: FieldValue,
    pub span: Span,
    pub source_scope: ScopeId,
    pub meta_info: Option<MetaInfo>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Label {
    pub value: String,
    pub is_atom: bool,
    pub span: Span,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CallExprEndKind {
    Inline,
    ClosingBracket(Span),
    End(Span),
    Generated,
}

#[derive(Clone, PartialEq, Debug)]
pub struct CallExpr<'a, ARGS: AsRef<[Argument]> = &'a mut [Argument]> {
    pub op_name: &'a str,
    pub args: ARGS,
    pub end_kind: CallExprEndKind,
    pub span: Span,
}

pub struct ParsedArgsIter<'a> {
    flags: Option<indexmap::map::Iter<'a, String, FieldValue>>,
    args: std::slice::Iter<'a, Argument>,
    positional_idx: usize,
}

pub struct ParsedArgsIterWithBoundedPositionals<'a> {
    op_name: &'a str,
    full_span: Span,
    iter: ParsedArgsIter<'a>,
    pargs_min: usize,
    pargs_max: usize,
}

pub enum ParsedArgValue<'a> {
    Flag(&'a str),
    NamedArg {
        key: &'a str,
        value: &'a FieldValue,
    },
    PositionalArg {
        arg: &'a Argument,
        idx: usize,
        value: &'a FieldValue,
    },
}

pub struct ParsedArg<'a> {
    pub value: ParsedArgValue<'a>,
    pub span: Span,
}

impl Argument {
    pub fn from_field_value(
        value: FieldValue,
        span: Span,
        source_scope: ScopeId,
    ) -> Self {
        Argument {
            value,
            span,
            source_scope,
            meta_info: None,
        }
    }
    pub fn generated_from_name(name: &str, scope: ScopeId) -> Self {
        Argument {
            value: FieldValue::Text(name.to_string()),
            source_scope: scope,
            span: Span::Generated,
            meta_info: None,
        }
    }
    pub fn generated_from_field_value(
        value: FieldValue,
        scope: ScopeId,
        meta_info: MetaInfo,
    ) -> Self {
        Argument {
            value,
            source_scope: scope,
            span: Span::Generated,
            meta_info: Some(meta_info),
        }
    }
    pub fn expect_plain(
        &self,
        op_name: &str,
    ) -> Result<&[u8], OperatorCreationError> {
        match &self.value {
            FieldValue::Text(v) => Ok(v.as_bytes()),
            FieldValue::Bytes(v) => Ok(v),
            _ => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{op_name}` expected a plaintext argument, not `{}`",
                    self.value.repr()
                ),
                self.span,
            )),
        }
    }
    pub fn expect_simple(
        &self,
        op_name: &str,
    ) -> Result<(), OperatorCreationError> {
        match &self.value {
            FieldValue::Argument(arg) => arg.expect_simple(op_name),
            FieldValue::Undefined
            | FieldValue::Null
            | FieldValue::Int(_)
            | FieldValue::Bool(_)
            | FieldValue::BigInt(_)
            | FieldValue::Float(_)
            | FieldValue::BigRational(_)
            | FieldValue::Text(_)
            | FieldValue::Bytes(_) => Ok(()),
            FieldValue::StreamValueId(_)
            | FieldValue::Array(_)
            | FieldValue::Object(_)
            | FieldValue::Error(_)
            | FieldValue::OpDecl(_)
            | FieldValue::Custom(_)
            | FieldValue::FieldReference(_)
            | FieldValue::SlicedFieldReference(_) => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{op_name}` expected a plain argument value, got type `{}`",
                    self.value.kind()
                ),
                self.span,
            ))
        }
    }
    pub fn expect_string(
        &self,
        op_name: &str,
    ) -> Result<&str, OperatorCreationError> {
        match &self.value {
            FieldValue::Text(v) => Ok(v),
            FieldValue::Bytes(_) => Err(OperatorCreationError::new_s(
                format!(
                    "argument for operator `{op_name}` must be valid utf-8",
                ),
                self.span,
            )),
            _ => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{op_name}` expected a plaintext argument, not `{}`",
                    self.value.repr()
                ),
                self.span,
            )),
        }
    }

    pub fn expect_arg_array(
        &self,
    ) -> Result<&Vec<Argument>, CliArgumentError> {
        if !matches!(&self.value, FieldValue::Array(Array::Argument(_))) {
            return Err(CliArgumentError::new_s(
                format!("expected argument array, not `{:?}`", self.value),
                self.span,
            ));
        }
        let FieldValue::Array(Array::Argument(sub_args)) = &self.value else {
            // avoids lifetime issue
            unreachable!()
        };
        Ok(sub_args)
    }
    pub fn expect_arg_array_mut(
        &mut self,
    ) -> Result<(&mut Vec<Argument>, &mut Option<MetaInfo>), CliArgumentError>
    {
        if !matches!(&self.value, FieldValue::Array(Array::Argument(_))) {
            return Err(CliArgumentError::new_s(
                format!("expected argument array, not `{:?}`", self.value),
                self.span,
            ));
        }
        let FieldValue::Array(Array::Argument(sub_args)) = &mut self.value
        else {
            // avoids lifetime issue
            unreachable!()
        };
        Ok((sub_args, &mut self.meta_info))
    }

    pub fn as_maybe_text(&self, sess: &mut SessionSetupData) -> MaybeTextCow {
        if let Some(MetaInfo::DenormalRepresentation(repr)) = &self.meta_info {
            return MaybeTextCow::TextRef(repr);
        }

        match &self.value {
            FieldValue::Text(text) => return MaybeTextCow::TextRef(text),
            FieldValue::Bytes(bytes) => return MaybeTextCow::BytesRef(bytes),
            FieldValue::Argument(arg) => return arg.as_maybe_text(sess),
            _ => (),
        }
        let mut ss = LazyRwLockGuard::NonRead(LazyRwLockWriteGuard::Plain(
            &mut sess.string_store,
        ));
        // TODO: incorporate more format quirks here
        let mut fmt = FormattingContext {
            ss: Some(&mut ss),
            fm: None,
            msm: None,
            rationals_print_mode: RationalsPrintMode::Dynamic,
            is_stream_value: false,
            rfk: RealizedFormatKey::default(),
        };
        let mut res = MaybeText::new();
        self.value.as_ref().format(&mut fmt, &mut res).unwrap();
        MaybeTextCow::from(res)
    }

    pub fn try_into_text(
        &self,
        op_name: &str,
        sess: &mut SessionSetupData,
    ) -> Result<Cow<str>, OperatorCreationError> {
        match self.as_maybe_text(sess) {
            MaybeTextCow::Text(text) => Ok(Cow::Owned(text)),
            MaybeTextCow::TextRef(text) => Ok(Cow::Borrowed(text)),
            MaybeTextCow::Bytes(_) | MaybeTextCow::BytesRef(_) => {
                Err(OperatorCreationError::new_s(
                    format!(
                        "argument for operator `{op_name}` must be valid utf-8",
                    ),
                    self.span,
                ))
            }
        }
    }

    pub fn try_into_str(
        self,
        op_name: &str,
        sess: &mut SessionSetupData,
    ) -> Result<String, OperatorCreationError> {
        if let FieldValue::Text(t) = self.value {
            return Ok(t);
        }
        Ok(self.try_into_text(op_name, sess)?.to_string())
    }

    pub fn expect_int<I>(
        &self,
        op_name: &str,
        fuzzy: bool,
    ) -> Result<I, OperatorCreationError>
    where
        I: PrimInt + TryFrom<i64>,
    {
        if let Some(v) = self.value.try_cast_int(fuzzy) {
            if let Ok(v) = I::try_from(v) {
                return Ok(v);
            }
        }
        Err(OperatorCreationError::new_s(
            format!(
                "operator `{op_name}` expected an integer, got `{}`",
                self.value.kind()
            ),
            self.span,
        ))
    }
}

impl Span {
    pub fn from_cli_arg(
        start: usize,
        end: usize,
        offset_start: usize,
        offset_end: usize,
    ) -> Self {
        Span::CliArg {
            start: NonMax::<u32>::from_usize(start),
            end: NonMax::<u32>::from_usize(end),
            offset_start: offset_start as u16,
            offset_end: offset_end as u16,
        }
    }
    pub fn from_single_arg_with_offset(
        cli_arg_idx: usize,
        offset_start: usize,
        offset_end: usize,
    ) -> Self {
        Span::CliArg {
            start: CliArgIdx::from_usize(cli_arg_idx),
            end: CliArgIdx::from_usize(cli_arg_idx + 1),
            offset_start: offset_start as u16,
            offset_end: offset_end as u16,
        }
    }
    pub fn from_single_arg(cli_arg_idx: usize, len: usize) -> Self {
        Span::CliArg {
            start: CliArgIdx::from_usize(cli_arg_idx),
            end: CliArgIdx::from_usize(cli_arg_idx + 1),
            offset_start: 0,
            offset_end: len as u16,
        }
    }
    pub fn reoffset(
        &self,
        cli_arg_offset_start: usize,
        cli_arg_offset_end: usize,
    ) -> Span {
        match self {
            Span::CliArg {
                start,
                end,
                offset_start: _,
                offset_end: _,
            } => Span::CliArg {
                start: *start,
                end: *end,
                offset_start: cli_arg_offset_start as u16,
                offset_end: cli_arg_offset_end as u16,
            },
            Span::FlagsObject => Span::FlagsObject,
            Span::Generated => Span::Generated,
            Span::Builtin => Span::Builtin,
            macro_exp @ Span::MacroExpansion { .. } => *macro_exp,
            env_var @ Span::EnvVar { .. } => *env_var,
        }
    }
    pub fn subslice_offsets(
        &self,
        cli_arg_offset_start: usize,
        cli_arg_offset_end: usize,
    ) -> Span {
        match *self {
            Span::CliArg {
                start,
                end,
                offset_start,
                offset_end: _,
            } => Span::CliArg {
                start,
                end,
                offset_start: offset_start + cli_arg_offset_start as u16,
                offset_end: offset_start + cli_arg_offset_end as u16,
            },
            Span::Generated => Span::Generated,
            Span::FlagsObject => Span::FlagsObject,
            Span::Builtin => Span::Builtin,
            macro_exp @ Span::MacroExpansion { .. } => macro_exp,
            env_var @ Span::EnvVar { .. } => env_var,
        }
    }
    pub fn slice_of_start(&self, cli_arg_offset_start: usize) -> Span {
        match *self {
            Span::CliArg {
                start,
                end,
                offset_start,
                offset_end,
            } => Span::CliArg {
                start,
                end,
                offset_start: offset_start + cli_arg_offset_start as u16,
                offset_end,
            },
            Span::Generated => Span::Generated,
            Span::FlagsObject => Span::FlagsObject,
            Span::Builtin => Span::Builtin,
            macro_exp @ Span::MacroExpansion { .. } => macro_exp,
            env_var @ Span::EnvVar { .. } => env_var,
        }
    }
    pub fn span_until(&self, end: Span) -> Option<Span> {
        match (*self, end) {
            (
                Span::CliArg {
                    start,
                    offset_start,
                    end: _,
                    offset_end: _,
                },
                Span::CliArg {
                    start: _,
                    offset_start: _,
                    end,
                    offset_end,
                },
            ) => Some(Span::CliArg {
                start,
                end,
                offset_start,
                offset_end,
            }),
            (
                Span::MacroExpansion { op_id: op_start },
                Span::MacroExpansion { op_id: op_end },
            ) if op_start == op_end => {
                Some(Span::MacroExpansion { op_id: op_start })
            }
            (Span::Generated, Span::Generated) => Some(Span::Generated),
            (Span::FlagsObject, Span::FlagsObject) => Some(Span::FlagsObject),
            (Span::Builtin, Span::Builtin) => Some(Span::Builtin),
            (_, _) => None,
        }
    }
}

fn parse_call_expr_meta<'a>(
    span: Span,
    first_sub_arg: Option<&'a Argument>,
    meta_info: Option<&'a MetaInfo>,
) -> Result<(&'a str, CallExprEndKind), CliArgumentError> {
    let Some(first_sub_arg) = first_sub_arg else {
        return Err(CliArgumentError::new(
            "call expression must contain at least one element",
            span,
        ));
    };
    let Some(val) = first_sub_arg.value.text_or_bytes() else {
        return Err(CliArgumentError::new(
            "call expression must start with an operator name identifier",
            span,
        ));
    };

    let op_name = val.to_str().map_err(|_| {
        CliArgumentError::new(
            "operator name must be valid utf-8",
            first_sub_arg.span,
        )
    })?;

    let end_kind = if let Some(MetaInfo::EndKind(end_kind)) = &meta_info {
        *end_kind
    } else {
        CallExprEndKind::Generated
    };

    Ok((op_name, end_kind))
}

impl<'a> CallExpr<'a, &'a [Argument]> {
    pub fn from_argument(arg: &'a Argument) -> Result<Self, CliArgumentError> {
        let sub_args = arg.expect_arg_array()?;

        let (op_name, end_kind) = parse_call_expr_meta(
            arg.span,
            sub_args.first(),
            arg.meta_info.as_ref(),
        )?;

        Ok(CallExpr {
            op_name,
            args: &sub_args[1..],
            end_kind,
            span: arg.span,
        })
    }
}

impl<'a> CallExpr<'a, &'a mut [Argument]> {
    pub fn from_argument_mut(
        arg: &'a mut Argument,
    ) -> Result<Self, CliArgumentError> {
        let span = arg.span;

        let (sub_args, meta_info) = arg.expect_arg_array_mut()?;

        let (arg1, args_rest) = sub_args.split_at_mut(1);

        let (op_name, end_kind) =
            parse_call_expr_meta(span, arg1.first(), meta_info.as_ref())?;

        Ok(CallExpr {
            op_name,
            end_kind,
            span,
            args: args_rest,
        })
    }
    pub fn split_flags_arg_normalized<'b>(
        &'b mut self,
        string_store: &StringStore,
        assume_flags_if_single_obj: bool,
    ) -> (Option<&'b mut ObjectKeysStored>, &'b mut [Argument]) {
        if !self.has_flags_arg(assume_flags_if_single_obj) {
            return (None, self.args);
        }
        let (arg, rest) = self.args.split_first_mut().unwrap();
        let FieldValue::Object(obj) = &mut arg.value else {
            unreachable!()
        };

        if let Object::KeysInterned(keys_interned) = &mut **obj {
            let mut keys_stored = ObjectKeysStored::new();
            for (k, v) in std::mem::take(keys_interned) {
                keys_stored.insert(string_store.lookup(k).to_string(), v);
            }
            *obj = Box::new(Object::KeysStored(keys_stored));
        }

        let Object::KeysStored(keys_stored) = &mut **obj else {
            unreachable!()
        };
        for v in keys_stored.values_mut() {
            if let FieldValue::Argument(_) = v {
                continue;
            }
            let value = std::mem::take(v);
            *v = FieldValue::Argument(Box::new(Argument {
                value,
                span: arg.span,
                source_scope: arg.source_scope,
                meta_info: None,
            }));
        }
        (Some(keys_stored), rest)
    }
}

impl<ARGS: AsRef<[Argument]>> CallExpr<'_, ARGS> {
    pub fn expect_flag(
        &self,
        key: &str,
        value: &FieldValue,
    ) -> Result<(), CliArgumentError> {
        let FieldValue::Argument(arg) = &value else {
            panic!("expected flags value to be an argument")
        };

        if arg.value != FieldValue::Undefined {
            return Err(self.error_reject_flag_value(key, arg.span));
        }
        Ok(())
    }
    pub fn has_flags_arg(&self, assume_flags_if_single_obj: bool) -> bool {
        let args = self.args.as_ref();

        let Some(first) = args.first() else {
            return false;
        };
        let FieldValue::Object(_) = first.value.deref_argument() else {
            return false;
        };
        if assume_flags_if_single_obj || first.span == Span::FlagsObject {
            return true;
        }
        let Some(second) = args.get(1) else {
            return false;
        };
        if second.value.kind() == FieldValueKind::Object {
            return true;
        }
        false
    }
    pub fn split_flags_arg(
        &self,
        assume_non_flags_if_single_obj: bool,
    ) -> (Option<&ObjectKeysStored>, &[Argument]) {
        if !self.has_flags_arg(assume_non_flags_if_single_obj) {
            return (None, self.args.as_ref());
        }
        let args = self.args.as_ref();
        let FieldValue::Object(obj) = &args[0].value.deref_argument() else {
            return (None, args);
        };
        let Object::KeysStored(obj) = &**obj else {
            return (None, args);
        };

        (Some(obj), &args[1..])
    }

    pub fn error_named_args_unsupported(
        &self,
        arg: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` does not support named arguments, '{arg}' given",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_positional_args_unsupported(
        &self,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` does not support positional arguments",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_positional_arg_not_plaintext(
        &self,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "positional arguments for operator `{}` must be plaintext",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_flag_unsupported(
        &self,
        flag: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` does not support flag '{flag}'",
                self.op_name,
            ),
            span,
        )
    }

    pub fn error_flags_unsupported(&self, span: Span) -> CliArgumentError {
        CliArgumentError::new_s(
            format!("operator `{}` does not support flags", self.op_name,),
            span,
        )
    }
    pub fn error_reject_flag_value(
        &self,
        flag: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "flag '{flag}' for operator `{}` does not accept a value",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_named_arg_unsupported(
        &self,
        key: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` does not support argument '{key}'",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_positional_arg_invalid_utf8(
        &self,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` argument value must be valid utf-8",
                self.op_name,
            ),
            span,
        )
    }

    pub fn error_arg_invalid_utf8(
        &self,
        argname: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` argument value for '{argname}' must be valid utf-8",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_positional_arg_invalid_int(
        &self,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` argument value must be a valid integer",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_arg_invalid_int(
        &self,
        argname: &str,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` argument value '{argname}' must be a valid integer",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_non_primitive_arg_unsupported(
        &self,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` only accepts primitive arguments",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_positional_arg_count_exceeded(
        &self,
        count_max: usize,
        span: Span,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` accepts at most {} positional arguments",
                self.op_name, count_max
            ),
            span,
        )
    }
    pub fn error_require_exact_positional_count(
        &self,
        count_max: usize,
    ) -> CliArgumentError {
        CliArgumentError::new_s(
            format!(
                "operator `{}` requires exactly {} positional arguments",
                self.op_name, count_max
            ),
            self.args
                .as_ref()
                .get(count_max)
                .map(|a| a.span)
                .unwrap_or(self.span),
        )
    }
    pub fn error_invalid_operator(&self) -> CliArgumentError {
        CliArgumentError::new_s(
            format!("unknown operator '{}'", self.op_name),
            self.span,
        )
    }
    pub fn parsed_args_iter(&self) -> ParsedArgsIter {
        let (flags, args) = self.split_flags_arg(true);
        ParsedArgsIter {
            flags: flags.map(|f| f.iter()),
            args: args.iter(),
            positional_idx: 0,
        }
    }
    pub fn parsed_args_iter_with_bounded_positionals(
        &self,
        pargs_min: usize,
        pargs_max: usize,
    ) -> ParsedArgsIterWithBoundedPositionals {
        ParsedArgsIterWithBoundedPositionals {
            op_name: self.op_name,
            full_span: self.span,
            iter: self.parsed_args_iter(),
            pargs_min,
            pargs_max,
        }
    }
    pub fn reject_args(&self) -> Result<(), OperatorCreationError> {
        if !self.args.as_ref().is_empty() {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` does not take any arguments",
                    self.op_name
                ),
                self.span,
            ));
        }
        Ok(())
    }
    pub fn require_at_most_one_plaintext_arg(
        &self,
    ) -> Result<Option<&[u8]>, OperatorCreationError> {
        if self.args.as_ref().is_empty() {
            return Ok(None);
        }
        if self.args.as_ref().len() != 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` does not accept more than one argument",
                    self.op_name
                ),
                self.span,
            ));
        }
        self.args.as_ref()[0].expect_plain(self.op_name).map(Some)
    }
    pub fn require_at_most_one_arg(
        &self,
    ) -> Result<Option<&Argument>, OperatorCreationError> {
        if self.args.as_ref().len() > 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires at most one parameter",
                    self.op_name
                ),
                self.span,
            ));
        }
        Ok(self.args.as_ref().first())
    }
    pub fn require_single_arg(
        &self,
    ) -> Result<&Argument, OperatorCreationError> {
        if self.args.as_ref().len() != 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires exactly one parameter",
                    self.op_name
                ),
                self.span,
            ));
        }
        Ok(&self.args.as_ref()[0])
    }
    pub fn require_nth_arg(
        &self,
        index: usize,
        purpose: &str,
    ) -> Result<&Argument, OperatorCreationError> {
        match self.args.as_ref().get(index) {
            Some(arg) => Ok(arg),
            None => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires {purpose} in argument {}",
                    self.op_name,
                    index + 1
                ),
                self.span,
            )),
        }
    }
    pub fn require_single_plaintext_arg(
        &self,
    ) -> Result<&[u8], OperatorCreationError> {
        self.require_single_arg()?.expect_plain(self.op_name)
    }
    pub fn require_single_string_arg_autoconvert(
        &self,
        sess: &mut SessionSetupData,
    ) -> Result<Cow<str>, OperatorCreationError> {
        let arg = self.require_single_arg()?;
        let res = arg.as_maybe_text(sess);
        match res {
            MaybeTextCow::TextRef(text) => Ok(Cow::Borrowed(text)),
            MaybeTextCow::Text(text) => Ok(Cow::Owned(text)),
            MaybeTextCow::Bytes(_) | MaybeTextCow::BytesRef(_) => {
                Err(OperatorCreationError::new_s(
                    format!(
                        "operator `{}` argument must be valid utf-8, got '{}'",
                        self.op_name,
                        res.as_ref().to_str_lossy()
                    ),
                    self.span,
                ))
            }
        }
    }
    pub fn require_single_plaintext_arg_autoconvert(
        &self,
        sess: &mut SessionSetupData,
    ) -> Result<MaybeTextCow, OperatorCreationError> {
        let arg = self.require_single_arg()?;
        Ok(arg.as_maybe_text(sess))
    }
    pub fn require_single_string_arg(
        &self,
    ) -> Result<&str, OperatorCreationError> {
        let arg = self.require_single_plaintext_arg()?;
        arg.to_str().map_err(|_| {
            OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires argument '{}' to be valid utf-8",
                    self.op_name,
                    arg.to_str_lossy()
                ),
                self.span,
            )
        })
    }
    pub fn require_single_int_param<I>(
        &self,
        fuzzy: bool,
    ) -> Result<I, OperatorCreationError>
    where
        I: PrimInt + TryFrom<i64>,
    {
        let arg = self.require_single_arg()?;
        arg.expect_int(self.op_name, fuzzy)
    }
    pub fn require_at_most_one_number_arg<I>(
        &self,
        fuzzy: bool,
    ) -> Result<Option<I>, OperatorCreationError>
    where
        I: PrimInt + TryFrom<i64>,
    {
        let Some(arg) = self.require_at_most_one_arg()? else {
            return Ok(None);
        };
        Ok(Some(arg.expect_int(self.op_name, fuzzy)?))
    }
    pub fn require_at_most_one_string_arg(
        &self,
    ) -> Result<Option<&str>, OperatorCreationError> {
        let arg = self.require_at_most_one_plaintext_arg()?;
        if arg.is_none() {
            return Ok(None);
        }
        Ok(Some(self.require_single_string_arg()?))
    }
    pub fn require_at_most_one_bool_arg(
        &self,
    ) -> Result<Option<bool>, OperatorCreationError> {
        if self.args.as_ref().is_empty() {
            return Ok(None);
        }
        if self.args.as_ref().len() > 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "expected at most one parameters for operator `{}`",
                    self.op_name,
                ),
                self.span,
            ));
        }
        let val = self.require_single_plaintext_arg()?;
        if let Some(b) = try_parse_bool(val) {
            Ok(Some(b))
        } else {
            Err(OperatorCreationError::new_s(
                format!(
                    "failed to `{}` parameter '{}' as bool",
                    self.op_name,
                    val.to_str_lossy(),
                ),
                self.span,
            ))
        }
    }
}

impl<'a> Iterator for ParsedArgsIter<'a> {
    type Item = ParsedArg<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, v)) = self.flags.as_mut().and_then(Iterator::next) {
            let mut span = Span::Generated;
            let value = if let FieldValue::Argument(arg) = v {
                span = arg.span;
                &arg.value
            } else {
                v
            };
            let value = if value == &FieldValue::Undefined {
                ParsedArgValue::NamedArg { key, value }
            } else {
                ParsedArgValue::Flag(key)
            };
            return Some(ParsedArg { value, span });
        }
        let arg = self.args.next()?;
        let idx = self.positional_idx;
        self.positional_idx += 1;
        Some(ParsedArg {
            value: ParsedArgValue::PositionalArg {
                arg,
                idx,
                value: &arg.value,
            },
            span: arg.span,
        })
    }
}

impl<'a> Iterator for ParsedArgsIterWithBoundedPositionals<'a> {
    type Item = Result<ParsedArg<'a>, OperatorCreationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(arg) = self.iter.next() else {
            if self.iter.positional_idx < self.pargs_min {
                return Some(Err(OperatorCreationError::new_s(
                    format!(
                        "operator `{}` needs at least {} positional argument{}",
                        self.op_name,
                        self.pargs_min,
                        if self.pargs_min > 1 {"s"} else {""}
                    ),
                    self.full_span,
                )));
            }
            return None;
        };
        match arg.value {
            ParsedArgValue::Flag(_) | ParsedArgValue::NamedArg { .. } => {
                Some(Ok(arg))
            }
            ParsedArgValue::PositionalArg { idx, .. } => {
                if idx > self.pargs_max {
                    if self.pargs_max == 0 {
                        return Some(Err(OperatorCreationError::new_s(
                                format!(
                                    "operator `{}` does not accept positional arguments",
                                    self.op_name
                                ),
                                arg.span,
                            )));
                    }
                    return Some(Err(OperatorCreationError::new_s(
                            format!(
                                "operator `{}` accepts at most {} positional argument{}",
                                self.op_name,
                                self.pargs_max,
                                if self.pargs_max > 1 {"s"} else {""}
                            ),
                            arg.span,
                        )));
                }
                Some(Ok(arg))
            }
        }
    }
}
