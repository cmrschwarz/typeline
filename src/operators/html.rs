extern crate html5ever;
extern crate markup5ever_rcdom as rcdom;

use std::borrow::Borrow;

use html5ever::serialize::{serialize, SerializeOpts};
use html5ever::tendril::TendrilSink;
use html5ever::tree_builder::TreeBuilderOpts;
use html5ever::{parse_document, ParseOpts};

use rcdom::{NodeData, RcDom, SerializableHandle};

pub fn foo() {
    let opts = ParseOpts {
        tree_builder: TreeBuilderOpts {
            drop_doctype: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let data = "<!DOCTYPE html>
                <html>
                    <body>
                        <a href=\"foo\"></a>
                    </body>
                </html>"
        .to_string();
    let dom = parse_document(RcDom::default(), opts)
        .from_utf8()
        .read_from(&mut data.as_bytes())
        .unwrap();

    let document = dom.document;
    let html = document.children.borrow()[0].clone();
    let body = html.children.borrow()[1].clone(); // Implicit head element at children[0].

    {
        let a = body.children.borrow()[0].clone();
        if let NodeData::Element { attrs, .. } = a.data.borrow() {
            attrs.borrow_mut()[0]
                .value
                .push_tendril(&From::from("#anchor"));
        }
    }

    let mut bytes = vec![];
    serialize(
        &mut bytes,
        &SerializableHandle::from(document),
        SerializeOpts::default(),
    )
    .unwrap();
    let result = String::from_utf8(bytes).unwrap();
    println!("{}", result);
}
