use rstest::rstest;
use typeline_core::{
    operators::{
        chunks::create_op_chunks,
        compute::{create_op_compute, create_op_to_int},
        file_reader::create_op_file_reader_custom,
        foreach::create_op_foreach,
        forkcat::create_op_forkcat,
        join::create_op_join,
        literal::{create_op_literal, create_op_v, Literal},
        regex::{create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
        sequence::{create_op_enum, create_op_seq, create_op_seqn},
    },
    options::{context_builder::ContextBuilder, session_setup::SessionSetupOptions},
    record_data::{array::Array, field_value::FieldValue},
    typeline_error::TypelineError,
    utils::test_utils::SliceReader,
};
use typeline_ext_utils::{
    collect::create_op_collect,
    dup::create_op_dup,
    eliminate_errors::create_op_eliminate_errors,
    explode::create_op_explode,
    flatten::create_op_flatten,
    head::create_op_head,
    max::create_op_max,
    primes::create_op_primes,
    string_utils::create_op_lines,
    sum::create_op_sum,
    tail::{create_op_tail, create_op_tail_add},
};

use crate::integration::UTILS_EXTENSION_REGISTRY;

#[test]
fn primes() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op_with_key("p", create_op_primes())
        .add_op(create_op_enum(0, 3, 1).unwrap())
        .add_op(create_op_select("p"))
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "3", "5"]);
    Ok(())
}

#[test]
fn primes_head() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_primes())
        .add_op(create_op_head(3))
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "3", "5"]);
    Ok(())
}

#[test]
fn primes_head_multi() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(10)?
        .add_op(create_op_seq(0, 3, 1)?)
        .add_op(create_op_foreach([create_op_primes(), create_op_head(3)]))
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "3", "5", "2", "3", "5", "2", "3", "5"]);
    Ok(())
}

#[test]
fn primes_head_multi_joined() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(10)?
        .add_op(create_op_seq(0, 3, 1)?)
        .add_op(create_op_foreach([
            create_op_primes(),
            create_op_head(3),
            create_op_join(Some(",".into()), None, false),
        ]))
        .run_collect_stringified()?;
    assert_eq!(res, ["2,3,5", "2,3,5", "2,3,5"]);
    Ok(())
}

#[test]
fn seq_tail_add() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_tail_add(7))
        .run_collect_stringified()?;
    assert_eq!(res, ["8", "9", "10"]);
    Ok(())
}

#[test]
fn primes_head_tail_add() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_primes())
        .add_op(create_op_tail_add(3))
        .add_op(create_op_head(3))
        .run_collect_stringified()?;
    assert_eq!(res, ["7", "11", "13"]);
    Ok(())
}

#[test]
fn head_tail_cli() -> Result<(), TypelineError> {
    let res = ContextBuilder::from_cli_arg_strings(
        SessionSetupOptions::with_extensions(UTILS_EXTENSION_REGISTRY.clone()),
        ["tl", "%bs=10", "primes", "tail=+3", "head=5"],
    )?
    .run_collect_as::<i64>()?;
    assert_eq!(res, [7, 11, 13, 17, 19]);
    Ok(())
}

#[test]
fn subtractive_head_multibatch() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_head(-5))
        .run_collect_as::<i64>()?;
    assert_eq!(res, [1, 2, 3, 4, 5]);
    Ok(())
}

#[test]
fn tail_multibatch() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)
        .unwrap()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_tail(4))
        .run_collect_as::<i64>()?;
    assert_eq!(res, [7, 8, 9, 10]);
    Ok(())
}

#[test]
fn multigroup_tail_multibatch() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)?
        .add_op(create_op_seq(1, 11, 1)?)
        .add_op(create_op_chunks(
            3,
            [create_op_tail(2), create_op_collect()],
        )?)
        .run_collect_stringified()?;
    assert_eq!(res, ["[2, 3]", "[5, 6]", "[8, 9]", "[10]"]);
    Ok(())
}

#[rstest]
#[case("{}", "null")]
#[case("[]", "[]")]
#[case("[{}]", "[{}]")]
fn explode_output_col(
    #[case] input: &str,
    #[case] output: &str,
) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_v(input).unwrap())
        .add_op(create_op_explode())
        .run_collect_stringified()?;
    assert_eq!(res, [output]);
    Ok(())
}

#[test]
fn explode_into_select() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_v("{'foo': 3}").unwrap())
        .add_op(create_op_explode())
        .add_op(create_op_select("foo"))
        .run_collect_stringified()?;
    assert_eq!(res, ["3"]);
    Ok(())
}

#[test]
fn flatten() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_v("[1,2,3]").unwrap())
        .add_op(create_op_flatten())
        .run_collect_stringified()?;
    assert_eq!(res, ["1", "2", "3"]);
    Ok(())
}

#[test]
fn object_flatten() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_v("{a: 3, b: '5'}").unwrap())
        .add_op(create_op_flatten())
        .run_collect_stringified()?;
    assert_eq!(res, [r#"["a", 3]"#, r#"["b", "5"]"#]);
    Ok(())
}

#[test]
fn flatten_duped_objects() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 3, 1).unwrap())
        .add_op(create_op_v("{a:3}").unwrap())
        .add_op(create_op_flatten())
        .run_collect()?;
    assert_eq!(
        res,
        std::iter::repeat(FieldValue::Array(Array::Mixed(
            [FieldValue::Text("a".to_string()), FieldValue::Int(3)].into()
        )))
        .take(3)
        .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn chunked_tail() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_chunks(3, [create_op_tail(1)]).unwrap())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [3, 6, 9, 10]);
    Ok(())
}

#[test]
fn chunked_max() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(create_op_chunks(3, [create_op_max()]).unwrap())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [3, 6, 9, 10]);
    Ok(())
}

#[test]
fn empty_max() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .add_op(create_op_seqn(1, 10, 1).unwrap())
        .add_op(
            create_op_chunks(3, [create_op_dup(0), create_op_max()]).unwrap(),
        )
        .run_collect_as::<i64>()?;
    assert_eq!(&res, &[] as &[i64]);
    Ok(())
}

#[test]
fn batched_ee() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(2)?
        .add_op(create_op_literal(Literal::Array(Array::Mixed(vec![
            FieldValue::Int(1),
            FieldValue::Int(2),
            FieldValue::Int(3),
            FieldValue::Text("a".into()),
        ]))))
        .add_op(create_op_flatten())
        .add_op(create_op_compute("_+1")?)
        .add_op(create_op_eliminate_errors())
        .add_op(create_op_max())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [4]);
    Ok(())
}

#[test]
fn batched_max_2() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(3)?
        .add_op(create_op_literal(Literal::Array(Array::Mixed(vec![
            FieldValue::Int(1),
            FieldValue::Int(2),
            FieldValue::Int(3),
            FieldValue::Text("a".into()),
            FieldValue::Int(5),
            FieldValue::Int(12),
            FieldValue::Text("b".into()),
            FieldValue::Int(7),
            FieldValue::Int(8),
        ]))))
        .add_op(create_op_flatten())
        .add_op(create_op_compute("_+1")?)
        .add_op(create_op_eliminate_errors())
        .add_op(create_op_max())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [13]);
    Ok(())
}

#[test]
fn multi_batch_primes_head() -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(7)?
        .add_op(create_op_primes())
        .add_op(create_op_head(10))
        .add_op(create_op_sum())
        .run_collect_stringified()?;
    assert_eq!(res, ["129"]);
    Ok(())
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

#[rstest]
#[case(1024)]
#[case(3)]
fn aoc2023_day1_part1(#[case] batch_size: usize) -> Result<(), TypelineError> {
    let res = ContextBuilder::without_exts()
        .set_batch_size(batch_size)?
        .add_ops([
            create_op_file_reader_custom(
                Box::new(SliceReader::new(AOC_2023_PART_1_INPUT.as_bytes())),
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
    assert_eq!(res, [444]);
    Ok(())
}
