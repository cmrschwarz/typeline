#![allow(dead_code, unused)] // HACK
extern crate crossbeam;
extern crate markup5ever_rcdom;
extern crate regex;
extern crate smallvec;
#[macro_use()]
extern crate lazy_static;

mod argument;
mod chain;
mod cli;
mod context;
mod document;
mod encoding;
mod operations;
mod options;
mod selenium;
mod transform;
mod xstr;
