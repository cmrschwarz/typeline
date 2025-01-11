use typeline_core::operators::{
    format::create_op_format,
    multi_op::create_multi_op,
    operator::Operator,
    regex::{
        create_op_regex_lines, create_op_regex_trim_trailing_newline,
        create_op_regex_with_opts, RegexOptions,
    },
};

use crate::exec::create_op_exec_from_strings;

// PERF: these commands could have much more efficient manual implementations
// (but the regex based ones are good enough for now)

pub fn create_op_lines() -> Box<dyn Operator> {
    // TODO: proper implementation
    create_multi_op([
        create_op_regex_trim_trailing_newline(),
        create_op_regex_lines(),
    ])
}

pub fn create_op_chars() -> Box<dyn Operator> {
    create_op_regex_with_opts(
        r".",
        RegexOptions {
            multimatch: true,
            dotall: true,
            ..Default::default()
        },
    )
    .unwrap()
}

pub fn create_op_trim() -> Box<dyn Operator> {
    create_op_regex_with_opts(
        r"^\s*(?<>.*?)\s*$",
        RegexOptions {
            dotall: true,
            ..Default::default()
        },
    )
    .unwrap()
}

pub fn create_op_to_tyson() -> Box<dyn Operator> {
    create_op_format("{:?}").unwrap()
}

// TODO: implement this properly
pub fn create_op_ls() -> Box<dyn Operator> {
    create_multi_op([
        create_op_exec_from_strings(["ls"]).unwrap(),
        create_op_lines(),
    ])
}
