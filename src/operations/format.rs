use bstr::BString;

use crate::{
    utils::string_store::StringStoreEntry,
    worker_thread_session::{FieldId, JobData},
};

use super::transform::{TransformData, TransformState};

pub enum FormatFillAlignment {
    Left,
    Center,
    Right,
}

pub enum FormatWidthSpec {
    Ref(Option<StringStoreEntry>),
    Value(usize),
}

pub enum NumberFormat {
    Plain,    // the default value representation
    Binary,   // print integers with base 2, e.g 101010 instead of 42
    Octal,    // print integers with base 8, e.g 52 instead of 42
    Hex,      // print integers in lower case hexadecimal, e.g 2a instead of 42
    UpperHex, // print integers in upper case hexadecimal, e.g 2A instead of 42
    LowerExp, // print numbers in upper case scientific notation, e.g. 4.2e1 instead of 42
    UpperExp, // print numbers in lower case scientific notation, e.g. 4.2E1 instead of 42
}

pub struct FormatKey {
    identifier: Option<StringStoreEntry>,
    fill: char,
    align: FormatFillAlignment,
    add_plus_sign: bool,
    zero_pad_numbers: bool,
    width: FormatWidthSpec,
    float_precision: FormatWidthSpec,
    alternate_form: bool, // prefix 0x for hex, 0o for octal and 0b for binary, pretty print objects / arrays
    number_format: NumberFormat,
    debug: bool,
    unicode: bool,
}

pub enum FormatPart {
    ByteLiteral(BString),
    TextLiteral(String),
    Key(FormatKey),
}

pub struct OpFormat {
    pub parts: Vec<FormatPart>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a [FormatPart],
}

pub fn setup_tf_format<'a>(
    sess: &mut JobData,
    op: &'a OpFormat,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    //TODO: cache field indices...
    let output_field = sess.record_mgr.add_field(tf_state.match_set_id, None);
    let tf = TfFormat {
        output_field,
        parts: &op.parts,
    };
    (TransformData::Format(tf), output_field)
}
