use bstring::bstr;

use crate::{
    field_data_iter::{
        AnySingleTypeFieldDataIter, FieldDataIterator, IntoSingleTypeFieldDataIterator,
    },
    options::argument::CliArgIdx,
    worker_thread_session::{JobData, TransformId},
};

use super::{operator_data::OperatorData, OperatorCreationError};

pub fn parse_print_op(
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "print takes no arguments (for now)",
            arg_idx,
        ));
    }
    Ok(OperatorData::Print)
}

const ERROR_TEXT: &'static str = "<Type Error>";

pub fn handle_print_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId) {
    let (batch, input_field) = sess.claim_batch(tf_id);
    let mut iter = sess.fields[input_field]
        .field_data
        .iter()
        .bounded(batch)
        .group_types();
    while let Some(iter) = iter.advance() {
        match iter {
            AnySingleTypeFieldDataIter::TextInline(text_iter) => {
                for (len, txt) in text_iter.header_to_len() {
                    for _ in 0..len {
                        println!("{txt}");
                    }
                }
            }
            AnySingleTypeFieldDataIter::BytesInline(_)
            | AnySingleTypeFieldDataIter::BytesBuffer(_)
            | AnySingleTypeFieldDataIter::BytesFile(_)
            | AnySingleTypeFieldDataIter::Object(_)
            | AnySingleTypeFieldDataIter::Unset(_)
            | AnySingleTypeFieldDataIter::Null(_)
            | AnySingleTypeFieldDataIter::EntryId(_)
            | AnySingleTypeFieldDataIter::Integer(_)
            | AnySingleTypeFieldDataIter::Reference(_)
            | AnySingleTypeFieldDataIter::Error(_)
            | AnySingleTypeFieldDataIter::Html(_) => {
                for _ in 0..iter.field_count() {
                    println!("{ERROR_TEXT}");
                }
            }
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
