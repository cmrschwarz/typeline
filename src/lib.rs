extern crate bstring;
extern crate crossbeam;
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
pub mod worker_thread;
pub mod document;
pub mod encoding;
pub mod operations;
pub mod options;
pub mod plattform;
pub mod scr_error;
pub mod selenium;
pub mod match_data;

#[cfg(test)]
mod tests;
