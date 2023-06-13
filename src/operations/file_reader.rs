use std::{
    fs::File,
    io::{BufRead, Read, StdinLock},
};

use crate::{
    field_data::{
        field_value_flags, FieldValueFormat, FieldValueHeader, FieldValueKind, FieldValueSize,
        INLINE_STR_MAX_LEN,
    },
    worker_thread_session::{
        EnterStreamModeFlag, FieldId, JobData, MatchSetId, StreamProducerDoneFlag,
    },
};

use super::{
    operator_base::OperatorId,
    transform_state::{TransformData, TransformId, TransformState},
};

pub enum FileType<'a> {
    StdinIsATty(StdinLock<'a>),
    StdinIsNoTty(StdinLock<'a>),
    File(File),
}

pub struct TfFileReader<'a> {
    file: FileType<'a>,
    file_consumed: bool,
    output_field: FieldId,
}

//TODO: add this as an operator aswell, only used for stdin for now
pub fn setup_tf_file_reader_as_entry_point<'a>(
    sess: &mut JobData<'a>,
    input_field: FieldId,
    output_field: FieldId,
    ms_id: MatchSetId,
    desired_batch_size: usize,
    file: FileType<'a>,
) -> (TransformState, TransformData<'a>) {
    let state = TransformState {
        input_field,
        available_batch_size: 1,
        stream_producers_slot_index: None,
        stream_successor: None,
        match_set_id: ms_id,
        successor: None,
        desired_batch_size,
        op_id: OperatorId::MAX,
        ordering_id: sess.claim_transform_ordering_id(),
    };

    let data = TransformData::FileReader(TfFileReader {
        file,
        output_field: output_field,
        file_consumed: false,
    });
    (state, data)
}

pub fn handle_tf_file_reader_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> EnterStreamModeFlag {
    if !fr.file_consumed {
        fr.file_consumed = true;
        sess.stream_producers.push_back(tf_id);
        sess.advertise_stream_batch_size(tf_id, 1);
        return true;
    }
    let (batch, _) = sess.claim_batch(tf_id);
    sess.fields[fr.output_field].field_data.push_unset(batch);
    sess.inform_successor_batch_available(tf_id, batch);
    false
}

pub fn handle_tf_file_reader_stream_mode(
    _sess: &mut JobData<'_>,
    _tf_id: TransformId,
    _fr: &mut TfFileReader,
) {
    //TODO: ???
}

pub fn handle_tf_file_reader_producer_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> StreamProducerDoneFlag {
    let fd = &mut sess.fields[fr.output_field].field_data;
    let data = &mut fd.data;
    let limit = INLINE_STR_MAX_LEN as u64;
    let res = match fr.file {
        FileType::StdinIsATty(ref mut f) => f.by_ref().read_until('\n' as u8, data),
        FileType::StdinIsNoTty(ref mut f) => f.by_ref().take(limit).read_to_end(data),
        FileType::File(ref mut f) => f.by_ref().take(limit).read_to_end(data),
    };
    let mut stream_end = true;
    match res {
        Ok(n) => {
            let flags = field_value_flags::DEFAULT;
            if n != 0 {
                stream_end = false;
            }
            fd.header.push(FieldValueHeader {
                fmt: FieldValueFormat {
                    kind: FieldValueKind::BytesInline,
                    flags,
                    size: n as FieldValueSize,
                },
                run_length: 1,
            });
        }
        Err(_io_error) => todo!(), //TODO: error handling
    }
    if !stream_end {
        sess.inform_successor_batch_available(tf_id, 1);
    }
    stream_end
}
