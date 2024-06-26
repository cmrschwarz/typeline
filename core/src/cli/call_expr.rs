use std::{fmt::Display, str::FromStr};

use bstr::ByteSlice;
use num::{FromPrimitive, PrimInt};

use crate::{
    operators::{errors::OperatorCreationError, operator::OperatorId},
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        setting::CliArgIdx,
    },
    utils::{
        debuggable_nonmax::DebuggableNonMaxU32, indexing_type::IndexingType,
        int_string_conversions::parse_int_with_units,
        string_store::StringStore,
    },
};

use super::{try_parse_bool, CliArgumentError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Span {
    #[default]
    Generated,
    Builtin,
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
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ArgumentValue<'a> {
    List(Vec<Argument<'a>>),
    Plain(&'a [u8]),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Argument<'a> {
    pub value: ArgumentValue<'a>,
    pub span: Span,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Label<'a> {
    pub value: &'a str,
    pub is_atom: bool,
    pub span: Span,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CallExprEndKind {
    Inline,
    ClosingBracket(Span),
    End(Span),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CallExpr<'a> {
    pub append_mode: bool,
    pub transparent_mode: bool,
    pub op_name: &'a str,
    pub label: Option<Label<'a>>,
    pub args: Vec<Argument<'a>>,
    pub end_kind: CallExprEndKind,
    pub span: Span,
}

pub struct ParsedArgsIter<'a> {
    args: &'a [Argument<'a>],
    flag_offset: Option<usize>,
    flags_over: bool,
    positional_arg_idx: usize,
}

pub struct ParsedArgsIterWithBoundedPositionals<'a> {
    op_name: &'a str,
    full_span: Span,
    iter: ParsedArgsIter<'a>,
    pargs_min: usize,
    pargs_max: usize,
}

pub enum ParsedArgValue<'a> {
    Flag(&'a [u8]),
    NamedArg {
        key: &'a [u8],
        value: &'a [u8],
    },
    PositionalArg {
        idx: usize,
        value: &'a ArgumentValue<'a>,
    },
}

pub struct ParsedArg<'a> {
    pub value: ParsedArgValue<'a>,
    pub span: Span,
}

impl Span {
    pub fn from_cli_arg(
        start: usize,
        end: usize,
        offset_start: usize,
        offset_end: usize,
    ) -> Self {
        Span::CliArg {
            start: DebuggableNonMaxU32::from_usize(start),
            end: DebuggableNonMaxU32::from_usize(end),
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
    pub fn subslice(
        &self,
        cli_arg_start: usize,
        cli_arg_count: usize,
        cli_arg_offset_start: usize,
        cli_arg_offset_end: usize,
    ) -> Span {
        match self {
            Span::CliArg {
                start,
                offset_start,
                end: _,
                offset_end: _,
            } => Span::CliArg {
                start: *start + CliArgIdx::from_usize(cli_arg_start),
                end: *start
                    + CliArgIdx::from_usize(cli_arg_start + cli_arg_count),
                offset_start: if cli_arg_start == 0 {
                    offset_start + cli_arg_offset_start as u16
                } else {
                    cli_arg_offset_start as u16
                },
                offset_end: cli_arg_offset_end as u16,
            },
            Span::MacroExpansion { op_id } => {
                Span::MacroExpansion { op_id: *op_id }
            }
            Span::Generated => Span::Generated,
            Span::Builtin => Span::Builtin,
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
            (Span::Builtin, Span::Builtin) => Some(Span::Builtin),
            (_, _) => None,
        }
    }
}

impl<'a> ArgumentValue<'a> {
    pub fn expect_plain(
        &self,
        op_name: &str,
        span: Span,
    ) -> Result<&'a [u8], OperatorCreationError> {
        match self {
            ArgumentValue::Plain(value) => Ok(value),
            ArgumentValue::List(_) => Err(OperatorCreationError::new_s(
                format!("operator `{op_name}` expected a plaintext argument, not a list"), span)
            ),
        }
    }
}

impl<'a> CallExpr<'a> {
    pub fn error_named_args_unsupported(
        &self,
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` does not support named arguments",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_positional_args_unsupported(
        &self,
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` does not support positional arguments",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_flag_value_unsupported(
        &self,
        flag: &[u8],
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` does not support flag '{}'",
                self.op_name,
                flag.to_str_lossy()
            ),
            span,
        )
    }
    pub fn error_named_arg_unsupported(
        &self,
        key: &[u8],
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` does not support argument '{}'",
                self.op_name,
                key.to_str_lossy()
            ),
            span,
        )
    }
    pub fn error_positional_arg_invalid_utf8(
        &self,
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` argument value must be valid utf-8",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_arg_invalid_utf8(
        &self,
        argname: &[u8],
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` argument value for '{}' must be valid utf-8",
                argname.to_str_lossy(),
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_positional_arg_invalid_int(
        &self,
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` argument value must be a valid integer",
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_arg_invalid_int(
        &self,
        argname: &[u8],
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` argument value '{}' must be a valid integer",
                argname.to_str_lossy(),
                self.op_name,
            ),
            span,
        )
    }
    pub fn error_list_arg_unsupported(
        &self,
        span: Span,
    ) -> OperatorCreationError {
        OperatorCreationError::new_s(
            format!(
                "operator `{}` does not accept list arguments",
                self.op_name
            ),
            span,
        )
    }
    pub fn error_invalid_operator(&self) -> CliArgumentError {
        CliArgumentError::new_s(
            format!("unknown operator '{}'", self.op_name),
            self.span,
        )
    }
    pub fn parsed_args_iter(&self) -> ParsedArgsIter {
        ParsedArgsIter {
            args: &self.args,
            flag_offset: None,
            flags_over: false,
            positional_arg_idx: 0,
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
        if !self.args.is_empty() {
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
    pub fn require_at_most_one_arg(
        &self,
    ) -> Result<Option<&'a [u8]>, OperatorCreationError> {
        if self.args.is_empty() {
            return Ok(None);
        }
        if self.args.len() != 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` does not accept more than one argument",
                    self.op_name
                ),
                self.span,
            ));
        }
        match self.args[0].value {
            ArgumentValue::Plain(value) => Ok(Some(value)),
            ArgumentValue::List(_) => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires a value argument, not a list",
                    self.op_name
                ),
                self.args[0].span,
            )),
        }
    }
    pub fn require_single_arg(
        &self,
    ) -> Result<&'a [u8], OperatorCreationError> {
        if self.args.len() != 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires exactly one parameter",
                    self.op_name
                ),
                self.span,
            ));
        }
        match self.args[0].value {
            ArgumentValue::Plain(value) => Ok(value),
            ArgumentValue::List(_) => Err(OperatorCreationError::new_s(
                format!(
                    "operator `{}` requires a value parameter, not a list",
                    self.op_name
                ),
                self.args[0].span,
            )),
        }
    }
    pub fn require_single_string_arg(
        &self,
    ) -> Result<&'a str, OperatorCreationError> {
        let arg = self.require_single_arg()?;
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
    pub fn require_single_number_param<I>(
        &self,
    ) -> Result<I, OperatorCreationError>
    where
        I: PrimInt
            + Display
            + FromPrimitive
            + FromStr<Err = std::num::ParseIntError>,
    {
        let value = self.require_single_string_arg()?;
        parse_int_with_units::<I>(value).map_err(|msg| {
            OperatorCreationError::new_s(
                format!(
                    "failed to parse `{}` parameter as an integer: {msg}",
                    self.op_name
                ),
                self.span,
            )
        })
    }
    pub fn require_at_most_one_number_arg<I>(
        &self,
    ) -> Result<Option<I>, OperatorCreationError>
    where
        I: PrimInt
            + Display
            + FromPrimitive
            + FromStr<Err = std::num::ParseIntError>,
    {
        let arg = self.require_at_most_one_arg()?;
        if arg.is_none() {
            return Ok(None);
        }
        Ok(Some(self.require_single_number_param()?))
    }
    pub fn require_at_most_one_string_arg(
        &self,
    ) -> Result<Option<&str>, OperatorCreationError> {
        let arg = self.require_at_most_one_arg()?;
        if arg.is_none() {
            return Ok(None);
        }
        Ok(Some(self.require_single_string_arg()?))
    }
    pub fn require_at_most_one_bool_arg(
        &self,
    ) -> Result<Option<bool>, OperatorCreationError> {
        if self.args.is_empty() {
            return Ok(None);
        }
        if self.args.len() > 1 {
            return Err(OperatorCreationError::new_s(
                format!(
                    "expected at most one parameters for operator `{}`",
                    self.op_name,
                ),
                self.span,
            ));
        }
        let val = self.require_single_arg()?;
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
    pub fn op_base_options(&self) -> OperatorBaseOptions {
        OperatorBaseOptions {
            argname: self.op_name.to_owned().into(),
            label: self.label.map(|l| l.value.to_owned().into()),
            span: self.span,
            transparent_mode: self.transparent_mode,
            append_mode: self.append_mode,
            output_is_atom: self.label.map(|l| l.is_atom).unwrap_or(false),
        }
    }
    pub fn op_base_options_interned(
        &self,
        string_store: &mut StringStore,
    ) -> OperatorBaseOptionsInterned {
        OperatorBaseOptionsInterned {
            argname: string_store.intern_cloned(self.op_name),
            label: self.label.map(|l| string_store.intern_cloned(l.value)),
            span: self.span,
            transparent_mode: self.transparent_mode,
            append_mode: self.append_mode,
            output_is_atom: self.label.map(|l| l.is_atom).unwrap_or(false),
        }
    }
}

impl<'a> Iterator for ParsedArgsIter<'a> {
    type Item = ParsedArg<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let first = self.args.first()?;
        if let ArgumentValue::Plain(value) = first.value {
            if let Some(flag_offset) = self.flag_offset {
                if let Some((mut begin, mut end, _char)) =
                    value[flag_offset..].char_indices().next()
                {
                    begin += flag_offset;
                    end += flag_offset;
                    let res = Some(ParsedArg {
                        value: ParsedArgValue::Flag(&value[begin..end]),
                        span: self.args[0].span.subslice(0, 1, begin, end),
                    });
                    self.flag_offset = Some(end);
                    return res;
                }
                self.flag_offset = None;
                self.args = &self.args[1..];
                return self.next();
            }
            if !self.flags_over && value == b"--" {
                self.flags_over = true;
                self.args = &self.args[1..];
                return self.next();
            }
            if !self.flags_over && value.starts_with(&[b'-']) {
                if let Some((begin, end, _char)) = value.char_indices().nth(1)
                {
                    self.flag_offset = Some(end);
                    return Some(ParsedArg {
                        value: ParsedArgValue::Flag(&value[begin..end]),
                        span: first.span.subslice(0, 1, 1, end),
                    });
                }
            }
        }
        self.flags_over = true;
        let idx = self.positional_arg_idx;
        self.args = &self.args[1..];
        self.positional_arg_idx += 1;
        Some(ParsedArg {
            value: ParsedArgValue::PositionalArg {
                idx,
                value: &first.value,
            },
            span: first.span,
        })
    }
}

impl<'a> Iterator for ParsedArgsIterWithBoundedPositionals<'a> {
    type Item = Result<ParsedArg<'a>, OperatorCreationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(arg) = self.iter.next() else {
            if self.iter.positional_arg_idx < self.pargs_min {
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
