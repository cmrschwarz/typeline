use bstring::bstr;

use crate::{
    field_data_iter::FieldDataIterator,
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
        .header_to_len()
    {
        todo!();
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
