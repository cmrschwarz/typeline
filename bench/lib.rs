#![cfg(feature = "unstable")]
#![feature(test)]
extern crate test;

use scr::{
    operations::{
        regex::{create_op_regex, create_op_regex_lines, RegexOptions},
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
fn regex_drop(b: &mut test::Bencher) {
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str("foo\nbar\nbaz\n", 1)
            .add_op(create_op_regex_lines())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get_data().unwrap().as_slice(), ["foo", "bar", "baz"]);
    });
}

#[bench]
fn large_batch(b: &mut test::Bencher) {
    let number_string_list: Vec<_> = (0..50000).into_iter().map(|n| n.to_string()).collect();
    let number_string_joined = number_string_list.iter().fold(String::new(), |mut f, n| {
        f.push_str(n.to_string().as_str());
        f.push_str("\n");
        f
    });
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .push_str(&number_string_joined, 1)
            .add_op(create_op_regex_lines())
            .add_op(create_op_regex("^[0-9]{1,3}$", RegexOptions::default()).unwrap())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(
            ss.get_data().unwrap().as_slice(),
            &number_string_list[0..1000]
        );
    });
}
