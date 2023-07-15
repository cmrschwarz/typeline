#[macro_use]
extern crate static_assertions;

pub extern crate bstr;

extern crate arrayvec;
extern crate crossbeam;
extern crate encoding_rs;
extern crate indexmap;
extern crate itertools;
extern crate lazy_static;
extern crate markup5ever_rcdom;
extern crate num;
extern crate rand;
extern crate regex;
extern crate regex_syntax;
extern crate smallstr;
extern crate smallvec;
extern crate thiserror;
extern crate url;
extern crate uuid;

#[macro_use]
pub mod utils;

pub mod chain;
pub mod cli;
pub mod context;
pub mod document;
pub mod encoding;
pub mod field_data;
pub mod operations;
pub mod options;
pub mod ref_iter;
pub mod scr_error;
pub mod selenium;
pub mod stream_value;
pub mod worker_thread;
pub mod worker_thread_session;
