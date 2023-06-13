#[macro_use]
extern crate static_assertions;

extern crate bstring;
extern crate atty;
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
pub mod encoding;
#[allow(dead_code)]
mod field_data;
pub mod operations;
pub mod options;
pub mod plattform;
pub mod scr_error;
mod scratch_vec;
pub mod selenium;
pub mod string_store;
pub mod universe;
pub mod worker_thread;
pub mod field_data_iterator;
mod worker_thread_session;

#[cfg(test)]
mod tests;
