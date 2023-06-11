use bstring::bstr;

use crate::{
    field_data_iter::FieldDataIterator,
    options::argument::CliArgIdx,
    scratch_vec::ScratchVec,
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

pub fn handle_print_batch_mode(sess: &mut JobData<'_>, tf_id: TransformId) {
    let tf = &mut sess.transforms[tf_id];
    let batch = tf.desired_batch_size.min(tf.available_batch_size);
    tf.available_batch_size -= batch;
    if tf.available_batch_size == 0 {
        sess.ready_queue.pop();
    }
    {
        let mut entries = ScratchVec::new(&mut sess.scratch_memory);
        let print_error_text = "<Type Error>";
        //TODO: much optmization much wow
        entries.extend(
            sess.fields[tf.input_field]
                .field_data
                .iter()
                .bounded(batch)
                .map(|(len, v)| match v {
                    crate::field_data::FieldValueRef::Text(sv) => match sv {
                        crate::field_data::FieldValueTextRef::Plain(txt) => (len, txt),
                        _ => (len, print_error_text),
                    },
                    _ => (len, print_error_text),
                }),
        );

        for (len, text) in entries.iter() {
            for _ in 0..*len {
                println!("{text}");
            }
        }
    }
    sess.inform_successor_batch_available(tf_id, batch);
}
