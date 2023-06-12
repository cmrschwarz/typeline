use bstring::bstr;

use crate::{
    field_data::FieldValueFlags,
    field_data_iterator::{BoundedFDIter, FDIter, FDIterator, FDTypedData, FDTypedRange},
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
    let mut iter = sess.fields[input_field].field_data.iter().bounded(batch, 0);
    while let Some(range) =
        iter.consume_typed_range_bwd(usize::MAX, FieldValueFlags::BYTES_ARE_UTF8)
    {
        match range.data {
            FDTypedData::TextInline(text) => {
                let mut data_end = text.len();
                for i in (0..range.field_count).rev() {
                    let data_start = data_end - range.headers[i].size as usize;
                    for _ in 0..range.headers[i].run_length {
                        println!("{}", &text[data_start..data_end]);
                    }
                    data_end = data_start;
                }
            }
            FDTypedData::Unset(_)
            | FDTypedData::Null(_)
            | FDTypedData::Integer(_)
            | FDTypedData::Reference(_)
            | FDTypedData::Error(_)
            | FDTypedData::Html(_)
            | FDTypedData::BytesInline(_)
            | FDTypedData::Object(_) => {
                for _ in 0..range.field_count {
                    println!("{}", ERROR_TEXT);
                }
            }
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
