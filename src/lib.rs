#![allow(dead_code, unused)] // HACK
extern crate smallvec;
extern crate markup5ever_rcdom;
extern crate crossbeam;

mod transform;
mod document;
mod context;
mod chain;
mod operations;
mod selenium;
mod encoding;
mod options;
mod cli;
