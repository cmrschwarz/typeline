use std::{
    iter::{Cloned, Peekable},
    marker::PhantomData,
};

use super::{
    call_expr::{Argument, CallExpr},
    parse_call_expr, CliArgumentError,
};

pub struct CallExprIter<'a, I> {
    input: I,
    _phantom: PhantomData<fn() -> &'a ()>,
}

impl<'a, I: Iterator<Item = Argument>> CallExprIter<'a, Peekable<I>> {
    pub fn new(input: Peekable<I>) -> Self {
        Self {
            input,
            _phantom: PhantomData,
        }
    }
    pub fn from_args_iter(input: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            input: input.into_iter().peekable(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, I: Iterator<Item = &'a Argument>>
    CallExprIter<'a, Peekable<Cloned<I>>>
{
    pub fn from_args_ref_iter(input: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            input: input.into_iter().cloned().peekable(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, I: Iterator<Item = Argument>> Iterator
    for CallExprIter<'a, Peekable<I>>
{
    type Item = Result<CallExpr, CliArgumentError>;

    fn next(&mut self) -> Option<Self::Item> {
        match parse_call_expr(&mut self.input) {
            Ok(None) => None,
            Ok(Some(expr)) => Some(Ok(expr)),
            Err(e) => Some(Err(e)),
        }
    }
}
