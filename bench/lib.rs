#![cfg(feature = "unstable")]
#![feature(test)]
extern crate test;

mod utils;
use crate::utils::*;

use scr::{
    bstr::ByteSlice,
    operations::{
        format::create_op_format,
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
        sequence::create_op_seq,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[bench]
fn empty_context(b: &mut test::Bencher) {
    b.iter(|| {
        let res = ContextBuilder::default().push_str("foobar", 1).run();
        assert!(matches!(res, Err(ScrError::ChainSetupError(_))));
    });
}

#[bench]
fn large_string_many_regex_matches(b: &mut test::Bencher) {
    const COUNT: usize = 10000;
    const MATCHES: usize = 1000;

    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str(&int_sequence_newline_separated(COUNT), 1)
            .add_op(create_op_regex_lines())
            .add_op(create_op_regex("^[0-9]{1,3}$", RegexOptions::default()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(
            ss.get_data().unwrap().as_slice(),
            &int_sequence_strings(MATCHES)
        );
    });
}

const LEN: usize = 1000;

#[bench]
fn plain_string_sink(b: &mut test::Bencher) {
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), int_sequence_strings(LEN));
    });
}

#[bench]
fn dummy_format(b: &mut test::Bencher) {
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_op(create_op_seq(0, LEN as i64, 1).unwrap())
            .add_op(create_op_format("{}".as_bytes().as_bstr()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), int_sequence_strings(LEN));
    });
}
