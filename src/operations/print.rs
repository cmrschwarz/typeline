use bstring::bstr;

use crate::{
    field_data::{FieldValueRef, FieldValueTextRef},
    field_data_iter::{FieldDataIterator, IntoFieldValueRefIter},
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
    for (len, v) in sess.fields[input_field]
        .field_data
        .iter()
        .bounded(batch)
        .value_refs()
        .len_only()
    {
        let (len, text) = match v {
            FieldValueRef::Text(sv) => match sv {
                FieldValueTextRef::Plain(txt) => (len, txt),
                _ => (len, ERROR_TEXT),
            },
            _ => (len, ERROR_TEXT),
        };
        for _ in 0..len {
            println!("{text}");
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
