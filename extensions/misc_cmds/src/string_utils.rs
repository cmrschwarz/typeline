use scr_core::operators::{
    operator::OperatorData,
    regex::{create_op_regex_with_opts, RegexOptions},
};

// PERF: these commands could have much more efficient manual implementations
// (but the regex based ones are good enough for now)

pub fn create_op_lines() -> OperatorData {
    create_op_regex_with_opts(
        r"[^\n]+",
        RegexOptions {
            multimatch: true,
            ..Default::default()
        },
    )
    .unwrap()
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
