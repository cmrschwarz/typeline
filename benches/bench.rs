use macro_rules_attribute::apply;
use paste::paste;
use scr::operators::{
    compute::create_op_to_int,
    file_reader::create_op_file_reader_custom,
    foreach::create_op_foreach,
    forkcat::create_op_forkcat,
    join::create_op_join,
    regex::{create_op_regex_with_opts, RegexOptions},
};
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
use scr_ext_utils::{
    head::create_op_head, string_utils::create_op_lines, sum::create_op_sum,
    tail::create_op_tail,
};
use std::{fmt::Write, io::Cursor, sync::LazyLock, time::Duration};

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
            .sample_size(20)
            .measurement_time(Duration::from_millis(1500))
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
    ($(#[$attrs:meta])* fn $fn_name:ident () $body:block) => {
        scrbench! {
            $(#[$attrs])*
            fn $fn_name() -> () $body
        }
    };
    ($(#[$attrs:meta])* fn $fn_name:ident () -> $ret:ty $body:block) => {
        paste! {
            fn [< raw_ $fn_name >]() -> $ret $body

            #[test]
            $(#[$attrs])*
            fn $fn_name() -> $ret {
                [< raw_ $fn_name >]()
            }

            inventory::submit!(Bench {
                name: stringify!($fn_name),
                bench_fn: || _ = [< raw_ $fn_name >](),
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

const AOC_2023_PART_1_INPUT: &str = r#"2qlljdqcbeight
eight47srvbfive
slconeightfoureight557m38
xvqeightwosixnine61eightsn2tdczfhx
msixonexch1twokjbdlhchqk1
112ninejlhhjmjzkzgdsix
6six7jr
878eightgvsqvzfthree
2jxzhlkhdktxfjjleightdfpgfxjv
mxbzgzg5three
"#;

#[apply(scrbench)]
#[cfg_attr(not(feature = "slow_tests"), ignore)]
fn aoc2023_day1_part1_large() -> Result<(), ScrError> {
    const COUNT: usize = 10000;
    let mut input = String::new();
    for _ in 0..COUNT {
        input.push_str(AOC_2023_PART_1_INPUT);
    }
    let res = ContextBuilder::without_exts()
        .set_batch_size(64)?
        .add_ops([
            create_op_file_reader_custom(
                Box::new(Cursor::new(input.into_bytes())),
                1,
            ),
            create_op_lines(),
            create_op_foreach([
                create_op_regex_with_opts(
                    "\\d",
                    RegexOptions {
                        multimatch: true,
                        ..Default::default()
                    },
                )?,
                create_op_forkcat([[create_op_head(1)], [create_op_tail(1)]]),
                create_op_join(None, None, false),
                create_op_to_int(),
            ]),
            create_op_sum(),
        ])
        .run_collect_as::<i64>()?;
    assert_eq!(res, [444 * COUNT as i64]);
    Ok(())
}
