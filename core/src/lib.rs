#![cfg_attr(feature = "unstable", feature(unsize, coerce_unsized))]
#![deny(unsafe_op_in_unsafe_fn)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

#[macro_use]
extern crate static_assertions;

extern crate arrayvec;
extern crate bitvec;
extern crate encoding_rs;
extern crate lazy_static;
extern crate memchr;
extern crate regex;
extern crate regex_syntax;
extern crate smallstr;
extern crate smallvec;
extern crate thin_vec;
extern crate thiserror;
extern crate unicode_ident;

#[cfg(feature = "repl")]
extern crate reedline;
#[cfg(feature = "repl")]
extern crate shlex;

#[macro_use]
pub mod utils;
pub mod chain;
pub mod cli;
pub mod context;
pub mod document;
pub mod extension;
pub mod job;
pub mod liveness_analysis;
pub mod operators;
pub mod options;
pub mod record_data;
pub mod scr_error;
pub mod selenium;
pub mod tyson;

pub const UNDEFINED_STR: &str = "undefined";
pub const NULL_STR: &str = "null";
