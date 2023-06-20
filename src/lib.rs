#[macro_use]
extern crate static_assertions;
extern crate arrayvec;
extern crate bstring;
extern crate crossbeam;
extern crate encoding_rs;
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
pub mod encoding;
pub mod field_data;
pub mod operations;
pub mod options;
pub mod scr_error;
pub mod selenium;
pub mod stream_field_data;
pub mod utils;
pub mod worker_thread;
pub mod worker_thread_session;

#[cfg(test)]
mod tests;
