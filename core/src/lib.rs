#![deny(unsafe_op_in_unsafe_fn)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
// pedantic
#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::semicolon_if_nothing_returned)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::match_on_vec_items)]
#![allow(clippy::similar_names)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::inline_always)]
#![allow(clippy::uninlined_format_args)]

// crates using macros. having this here helps out rust_analyzer
extern crate metamatch;
extern crate static_assertions;

#[cfg(miri)]
extern crate bitvec_miri as bitvec;

#[cfg(not(miri))]
extern crate bitvec_non_miri as bitvec;

#[cfg(feature = "debug_log")]
pub mod debug_log;

#[macro_use]
pub mod utils;
pub mod chain;
pub mod cli;
pub mod context;
pub mod extension;
pub mod job;
pub mod liveness_analysis;
pub mod operators;
pub mod options;
pub mod record_data;
pub mod repl_prompt;
pub mod typeline_error;
pub mod tyson;

pub const UNDEFINED_STR: &str = "undefined";
pub const NULL_STR: &str = "null";
