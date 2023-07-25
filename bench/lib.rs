#![cfg(feature = "unstable")]
#![feature(string_extend_from_within)]
#![feature(test)]
extern crate test;

mod utils;
use crate::utils::*;

use scr::{
    operators::{
        format::create_op_format,
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::{ContextualizedScrError, ScrError},
};

#[bench]
fn empty_context(b: &mut test::Bencher) {
    b.iter(|| {
        let res = ContextBuilder::default().push_str("foobar", 1).run();
        assert!(matches!(
            res,
            Err(ContextualizedScrError {
                err: ScrError::ChainSetupError(_),
                contextualized_message: _
            })
        ));
    });
}

const LEN: usize = 2000;

#[bench]
fn plain_string_sink(b: &mut test::Bencher) {
    let res = int_sequence_strings(LEN);
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn regex_lines(b: &mut test::Bencher) {
    const COUNT: usize = 1000;
    let input = int_sequence_newline_separated(COUNT);
    let res = int_sequence_strings(COUNT);
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str(&input, 1)
            .add_op(create_op_regex_lines())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn regex_lines_plus_drop_uneven(b: &mut test::Bencher) {
    const COUNT: usize = 1000;
    let input = int_sequence_newline_separated(COUNT);
    let res: Vec<String> = int_sequence_strings(COUNT)
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| if i % 2 == 0 { Some(v) } else { None })
        .collect();
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str(&input, 1)
            .add_op(create_op_regex_lines())
            .add_op(
                create_op_regex("^.*[02468]$", RegexOptions::default())
                    .unwrap(),
            )
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn dummy_format(b: &mut test::Bencher) {
    let res = int_sequence_strings(LEN);
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
            .add_op(create_op_format("{}".as_bytes()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn format_twice(b: &mut test::Bencher) {
    let mut res = int_sequence_strings(LEN);
    for v in res.iter_mut() {
        v.extend_from_within(0..);
    }
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
            .add_op(create_op_format("{}{}".as_bytes()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn regex_drop_uneven_into_format_twice(b: &mut test::Bencher) {
    const COUNT: usize = 1000;
    let input = int_sequence_newline_separated(COUNT);
    let res: Vec<String> = int_sequence_strings(COUNT)
        .into_iter()
        .enumerate()
        .filter_map(|(i, mut v)| {
            if i % 2 == 0 {
                v.extend_from_within(0..);
                Some(v)
            } else {
                None
            }
        })
        .collect();
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str(&input, 1)
            .add_op(create_op_regex_lines())
            .add_op(
                create_op_regex("^.*[02468]$", RegexOptions::default())
                    .unwrap(),
            )
            .add_op(create_op_format("{}{}".as_bytes()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}

#[bench]
fn seq_into_regex_drop_unless_seven(b: &mut test::Bencher) {
    const COUNT: usize = 10000;
    let res: Vec<&str> = int_sequence_strings(COUNT)
        .into_iter()
        .filter_map(|v| if v.contains("7") { Some("7") } else { None })
        .collect();
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, COUNT as i64, 1).unwrap())
            .add_op(create_op_regex("7", RegexOptions::default()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), res);
    });
}
