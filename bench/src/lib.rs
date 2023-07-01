#![cfg(test)]
#![feature(test)]

extern crate test;

use scr::{
    document::DocumentSource,
    operations::{
        regex::create_op_regex_lines,
        string_sink::{create_op_string_sink, StringSinkHandle},
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
};

#[bench]
fn empty_context(b: &mut test::Bencher) {
    b.iter(|| {
        let res = ContextBuilder::default()
            .add_doc(DocumentSource::String("foobar".to_owned()))
            .run();
        assert!(matches!(res, Err(ScrError::ChainSetupError(_))));
    });
}

#[bench]
fn regex_drop(b: &mut test::Bencher) {
    b.iter(|| {
        let ss = StringSinkHandle::new();
        ContextBuilder::default()
            .add_doc(DocumentSource::String("foo\nbar\nbaz\n".to_owned()))
            .add_op(create_op_regex_lines())
            .add_op(create_op_string_sink(&ss))
            .run()
            .unwrap();
        assert_eq!(ss.get().as_slice(), ["foo", "bar", "baz"]);
    });
}
