#[macro_use]
extern crate static_assertions;

extern crate encoding_rs;
extern crate bstring;
extern crate crossbeam;
extern crate indexmap;
extern crate lazy_static;
extern crate markup5ever_rcdom;
extern crate num;
extern crate regex;
extern crate smallvec;
extern crate thiserror;
extern crate url;

pub mod chain;
pub mod cli;
pub mod context;
pub mod document;
pub mod operations;
pub mod options;
pub mod encoding;

pub mod scr_error;
pub mod utils;
pub mod selenium;

pub mod worker_thread;
pub mod stream_field_data;
pub mod worker_thread_session;

#[cfg(test)]
mod tests;

mod field_data;
