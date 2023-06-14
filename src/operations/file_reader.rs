use std::{
    cell::RefCell,
    fs::File,
    io::{BufRead, Read, StdinLock},
    rc::Rc,
};

use crate::{
    field_data::{
        field_value_flags, FieldValueFormat, FieldValueHeader, FieldValueKind, FieldValueSize,
        INLINE_STR_MAX_LEN,
    },
    stream_field_data::{StreamFieldValue, StreamFieldValueData, StreamValueId},
    worker_thread_session::{
        EnterStreamModeFlag, FieldId, JobData, MatchSetId, StreamProducerDoneFlag,
    },
};

use super::{
    errors::io_error_to_op_error,
    operator::OperatorId,
    transform::{TransformData, TransformId, TransformState},
};

pub enum FileType<'a> {
    StdinIsATty(StdinLock<'a>),
    StdinIsNoTty(StdinLock<'a>),
    File(File),
}

pub struct TfFileReader<'a> {
    file: Option<FileType<'a>>,
    output_field: FieldId,
    stream_value: StreamValueId,
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
        match_set_id: ms_id,
        successor: None,
        desired_batch_size,
        op_id: OperatorId::MAX,
        ordering_id: sess.claim_transform_ordering_id(),
    };

    let data = TransformData::FileReader(TfFileReader {
        file: Some(file),
        output_field: output_field,
        stream_value: 0,
    });
    (state, data)
}

fn read_chunk(
    target: &mut Vec<u8>,
    file: &mut FileType,
    limit: usize,
) -> Result<(usize, bool), std::io::Error> {
    let size;
    let eof;
    match file {
        FileType::StdinIsATty(ref mut f) => {
            size = f
                .by_ref()
                .take(limit as u64)
                .read_until('\n' as u8, target)?;
            eof = if size > 0 && target[size] == '\n' as u8 {
                false
            } else {
                true
            };
        }
        FileType::StdinIsNoTty(ref mut f) => {
            size = f.by_ref().take(limit as u64).read_to_end(target)?;
            eof = size > 0;
        }
        FileType::File(ref mut f) => {
            size = f.by_ref().take(limit as u64).read_to_end(target)?;
            eof = size > 0;
        }
    };
    Ok((size, eof))
}

pub fn handle_tf_file_reader_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> EnterStreamModeFlag {
    if let Some(f) = &mut fr.file {
        let out_field = &mut sess.fields[fr.output_field];
        let size_before = out_field.field_data.data.len();
        //HACK: this should require unsafe, because we could be lying about the layout here
        let res = read_chunk(&mut out_field.field_data.data, f, INLINE_STR_MAX_LEN);
        let chunk_size = match res {
            Ok((size, eof)) => {
                if eof {
                    out_field.field_data.header.push(FieldValueHeader {
                        fmt: FieldValueFormat {
                            kind: FieldValueKind::BytesInline,
                            flags: field_value_flags::DEFAULT,
                            size: size as FieldValueSize,
                        },
                        run_length: 1,
                    });
                    sess.inform_successor_batch_available(tf_id, 1);
                    fr.file.take();
                    return false;
                }
                size
            }
            Err(err) => {
                let err = io_error_to_op_error(&sess.transforms, tf_id, err);
                out_field.field_data.push_error(err, 1);
                fr.file.take();
                sess.inform_successor_batch_available(tf_id, 1);
                return false;
            }
        };
        let mut buf = Vec::with_capacity(chunk_size);
        buf.extend_from_slice(&out_field.field_data.data[size_before..size_before + chunk_size]);
        out_field
            .stream_field_data
            .values
            .push_back(Rc::new(RefCell::new(StreamFieldValue {
                data: StreamFieldValueData::BytesBuffer(buf),
                done: false,
                bytes_are_utf8: false,
            })));
        fr.stream_value =
            out_field.stream_field_data.values.len() + out_field.stream_field_data.id_offset;
        out_field
            .field_data
            .push_stream_value_id(fr.stream_value, 1);
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
    let input = &mut sess.fields[fr.output_field];
    let sfd = &mut input.stream_field_data;
    let mut sv = sfd.get_value_mut(&sfd.values, fr.stream_value);
    let mut update = true;
    let res = match &mut sv.data {
        StreamFieldValueData::BytesChunk(bc) => {
            bc.clear();
            read_chunk(bc, fr.file.as_mut().unwrap(), INLINE_STR_MAX_LEN)
        }
        StreamFieldValueData::BytesBuffer(bb) => {
            update = false;
            read_chunk(bb, fr.file.as_mut().unwrap(), INLINE_STR_MAX_LEN)
        }
        StreamFieldValueData::Error(_) => {
            fr.file.take();
            Ok((0, true))
        }
    };
    if update {
        sfd.updates.push(fr.stream_value);
    }
    match res {
        Ok((_size, eof)) => {
            if eof {
                sv.done = true;
                return true;
            }
            false
        }
        Err(err) => {
            let err = io_error_to_op_error(&sess.transforms, tf_id, err);
            sv.data = StreamFieldValueData::Error(err);
            sv.done = true;
            true
        }
    }
}
