#![deny(unsafe_op_in_unsafe_fn)]

#[macro_use]
extern crate static_assertions;

extern crate arrayvec;
extern crate bitvec;
extern crate encoding_rs;
extern crate lazy_static;
extern crate num_traits;
extern crate rand;
extern crate reedline;
extern crate regex;
extern crate regex_syntax;
extern crate shlex;
extern crate smallstr;
extern crate smallvec;
extern crate thiserror;
extern crate uuid;

#[macro_use]
pub mod utils;

pub mod chain;
pub mod cli;
pub mod context;
pub mod document;
pub mod job_session;
pub mod liveness_analysis;
pub mod operators;
pub mod options;
pub mod record_data;
pub mod ref_iter;
pub mod scr_error;
pub mod selenium;
pub mod stream_value;
pub mod worker_thread;
