use macro_rules_attribute::apply;
use paste::paste;
use scr_core::{
    operators::{
        format::create_op_format,
        regex::{create_op_regex, create_op_regex_lines},
        sequence::create_op_seq,
    },
    options::context_builder::ContextBuilder,
    scr_error::{ContextualizedScrError, ScrError},
    utils::test_utils::{
        int_sequence_newline_separated, int_sequence_strings,
    },
};
use scr_ext_utils::string_utils::create_op_lines;
use std::{fmt::Write, sync::LazyLock, time::Duration};

#[allow(unused)]
struct Bench {
    name: &'static str,
    bench_fn: fn(),
}
inventory::collect!(Bench);

#[allow(unused)]
fn main() {
    let mut criterion: criterion::Criterion<_> =
        (criterion::Criterion::default())
            .warm_up_time(Duration::from_millis(100))
            .measurement_time(Duration::from_millis(300))
            .noise_threshold(0.10)
            .configure_from_args();
    for b in inventory::iter::<Bench>() {
        criterion.bench_function(&b.name, |c| c.iter(b.bench_fn));
    }
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}

// generates a `#[test]` fn and a criterion bench for the annotated function
macro_rules! scrbench {
    (fn $fn_name:ident () $body:tt) => {
        paste! {
            fn [< raw_ $fn_name >]() $body

            #[test]
            fn $fn_name() {
                [< raw_ $fn_name >]();
            }

            inventory::submit!(Bench {
                name: stringify!($fn_name),
                bench_fn: [< raw_ $fn_name >],
            });
        }
    };
}

#[apply(scrbench)]
fn empty_context() {
    let res = ContextBuilder::without_exts().push_str("foobar", 1).run();

    assert!(matches!(
        res,
        Err(ContextualizedScrError {
            err: ScrError::ChainSetupError(_),
            contextualized_message: _
        })
    ));
}

#[apply(scrbench)]
fn plain_string_sink() {
    const LEN: usize = 2000;
    const EXPECTED: LazyLock<Vec<String>> =
        LazyLock::new(|| int_sequence_strings(0..LEN));

    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(&res, &**EXPECTED);
}

#[apply(scrbench)]
fn regex_lines() {
    const COUNT: usize = 1000;
    const INPUT: LazyLock<String> =
        LazyLock::new(|| int_sequence_newline_separated(0..COUNT));

    const EXPECTED: LazyLock<Vec<String>> =
        LazyLock::new(|| int_sequence_strings(0..COUNT));

    let res = ContextBuilder::without_exts()
        .push_str(&INPUT, 1)
        .add_op(create_op_lines())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(res, &**EXPECTED);
}

#[apply(scrbench)]
fn regex_lines_plus_drop_uneven() {
    const COUNT: usize = 1000;
    const INPUT: LazyLock<String> =
        LazyLock::new(|| int_sequence_newline_separated(0..COUNT));

    const EXPECTED: LazyLock<Vec<String>> = LazyLock::new(|| {
        int_sequence_strings(0..COUNT)
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| if i % 2 == 0 { Some(v) } else { None })
            .collect()
    });

    let res = ContextBuilder::without_exts()
        .push_str(&INPUT, 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^.*[02468]$").unwrap())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(res, &**EXPECTED);
}

#[apply(scrbench)]
fn dummy_format() {
    const LEN: usize = 2000;
    const EXPECTED: LazyLock<Vec<String>> =
        LazyLock::new(|| int_sequence_strings(0..LEN));

    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
        .add_op(create_op_format("{}").unwrap())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(res, &**EXPECTED);
}

#[apply(scrbench)]
fn format_twice() {
    const LEN: usize = 2000;
    const EXPECTED: LazyLock<Vec<String>> = LazyLock::new(|| {
        let mut expected = Vec::new();
        for i in 0..LEN {
            expected.push(format!("{i}{i}"));
        }
        expected
    });

    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
        .add_op(create_op_format("{}{}").unwrap())
        .run_collect_stringified()
        .unwrap();

    assert_eq!(res, &**EXPECTED);
}

#[apply(scrbench)]
fn regex_drop_uneven_into_format_twice() {
    const COUNT: usize = 1000;
    const INPUT: LazyLock<String> = LazyLock::new(|| {
        let mut input = String::new();
        for i in 0..COUNT {
            input.write_fmt(format_args!("{i}\n")).unwrap();
        }
        input
    });

    const EXPECTED: LazyLock<Vec<String>> = LazyLock::new(|| {
        let mut expected = Vec::new();
        for i in 0..COUNT {
            if i % 2 == 0 {
                expected.push(format!("{i}{i}"));
            }
        }
        expected
    });

    let res = ContextBuilder::without_exts()
        .push_str(&INPUT, 1)
        .add_op(create_op_regex_lines())
        .add_op(create_op_regex("^.*[02468]$").unwrap())
        .add_op(create_op_format("{}{}").unwrap())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(res, &**EXPECTED);
}

#[apply(scrbench)]
fn seq_into_regex_drop_unless_seven() {
    const COUNT: usize = 10000;
    const EXPECTED: LazyLock<Vec<&str>> = LazyLock::new(|| {
        int_sequence_strings(0..COUNT)
            .into_iter()
            .filter_map(|v| if v.contains("7") { Some("7") } else { None })
            .collect()
    });

    let res = ContextBuilder::without_exts()
        .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
        .add_op(create_op_regex("7").unwrap())
        .run_collect_stringified()
        .unwrap();
    assert_eq!(res, &**EXPECTED);
}
