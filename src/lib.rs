#![allow(dead_code, unused)] // HACK
extern crate bstring;
extern crate crossbeam;
extern crate markup5ever_rcdom;
extern crate num;
extern crate regex;
#[macro_use()]
extern crate smallvec;
#[macro_use()]
extern crate lazy_static;

mod chain;
mod cli;
mod context;
mod document;
mod encoding;
mod operations;
mod options;
mod plattform;
mod selenium;
