use scr_core::operators::{
    multi_op::create_multi_op,
    operator::OperatorData,
    regex::{
        create_op_regex_lines, create_op_regex_trim_trailing_newline,
        create_op_regex_with_opts, RegexOptions,
    },
};

// PERF: these commands could have much more efficient manual implementations
// (but the regex based ones are good enough for now)

pub fn create_op_lines() -> OperatorData {
    // TODO: proper implementation
    create_multi_op([
        create_op_regex_trim_trailing_newline(),
        create_op_regex_lines(),
    ])
}

pub fn create_op_chars() -> OperatorData {
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

pub fn create_op_trim() -> OperatorData {
    create_op_regex_with_opts(
        r"^\s*(?<>.*?)\s*$",
        RegexOptions {
            dotall: true,
            ..Default::default()
        },
    )
    .unwrap()
}
