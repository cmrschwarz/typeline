extern crate bstring;
extern crate crossbeam;
extern crate lazy_static;
extern crate markup5ever_rcdom;
extern crate num;
extern crate regex;
extern crate smallvec;
extern crate thiserror;
extern crate url;
extern crate indexmap;

pub mod chain;
pub mod cli;
pub mod string_store;
pub mod context;
pub mod worker_thread;
pub mod document;
pub mod encoding;
pub mod operations;
pub mod options;
pub mod plattform;
pub mod scr_error;
pub mod selenium;
pub mod universe;
mod match_value;
mod match_value_into_iter;
mod sync_variant;
mod worker_thread_session;
#[allow(dead_code)]
mod field_data;
mod scratch_vec;

#[cfg(test)]
mod tests;
